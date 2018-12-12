using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AzureStorage;
using AzureStorage.Tables;
using Lykke.Common.Log;
using Lykke.SettingsReader;
using Lykke.Service.TradeVolumes.Core;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Core.Repositories;
using Lykke.Service.TradeVolumes.AzureRepositories.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Service.TradeVolumes.AzureRepositories
{
    public class TradeVolumesRepository : ITradeVolumesRepository
    {
        private const string _partitionKey = "PartitionKey";
        private const string _rowKey = "RowKey";

        private readonly IReloadingManager<string> _connectionStringManager;
        private readonly IAssetsDictionary _assetsDictionary;
        private readonly ILogFactory _logFactory;
        private readonly CloudTableClient _tableClient;
        private readonly TimeSpan _timeout = TimeSpan.FromMinutes(5);
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private readonly ConcurrentDictionary<string, INoSQLTableStorage<TradeVolumeEntity>> _azureTables = new ConcurrentDictionary<string, INoSQLTableStorage<TradeVolumeEntity>>();

        public TradeVolumesRepository(
            IReloadingManager<string> connectionStringManager,
            IAssetsDictionary assetsDictionary,
            ILogFactory logFactory)
        {
            _connectionStringManager = connectionStringManager;
            _assetsDictionary = assetsDictionary;
            _logFactory = logFactory;
            _tableClient = CloudStorageAccount.Parse(connectionStringManager.CurrentValue).CreateCloudTableClient();
        }

        public async Task NotThreadSafeTradeVolumesUpdateAsync(
            DateTime dateTime,
            string baseAssetId,
            string quotingAssetId,
            ICollection<(string, double, double)> userVolumes,
            ICollection<(string, string, double, double)> waletVolumes)
        {
            var items = new List<TradeVolumeEntity>(userVolumes.Count + waletVolumes.Count);
            foreach (var userVolume in userVolumes)
            {
                string userId = userVolume.Item1;
                double baseVolume = userVolume.Item2;
                double quotingVolume = userVolume.Item3;
                items.Add(
                    TradeVolumeEntity.ByUser.Create(
                        dateTime,
                        userId,
                        baseAssetId,
                        baseVolume,
                        quotingAssetId,
                        quotingVolume));
            }
            foreach (var walletVolume in waletVolumes)
            {
                string userId = walletVolume.Item1;
                string walletId = walletVolume.Item2;
                double baseVolume = walletVolume.Item3;
                double quotingVolume = walletVolume.Item4;
                items.Add(
                    TradeVolumeEntity.ByWallet.Create(
                        dateTime,
                        userId,
                        walletId,
                        baseAssetId,
                        baseVolume,
                        quotingAssetId,
                        quotingVolume));
            }
            var baseStorage = await GetStorageAsync(baseAssetId, quotingAssetId);
            await baseStorage.InsertOrReplaceAsync(items);
        }

        public async Task<Dictionary<string, double[]>> GetUserWalletsTradeVolumesAsync(
            DateTime date,
            IEnumerable<string> userIds,
            IEnumerable<string> walletIds,
            string baseAssetId,
            string quotingAssetId)
        {
            var storage = await GetStorageAsync(baseAssetId, quotingAssetId);

            string partitionFilter = TableQuery.GenerateFilterCondition(
                _partitionKey,
                QueryComparisons.Equal,
                TradeVolumeEntity.GeneratePartitionKey(date));
            var sb = new StringBuilder($"({partitionFilter}) and (");
            bool isFirstRowFilter = true;
            foreach (var userId in userIds)
            {
                if (isFirstRowFilter)
                    isFirstRowFilter = false;
                else
                    sb.Append(" or ");
                string userRowKey = TradeVolumeEntity.ByUser.GenerateRowKey(userId);
                string userRowFilter = TableQuery.GenerateFilterCondition(_rowKey, QueryComparisons.Equal, userRowKey);
                sb.Append(userRowFilter);
            }
            foreach (var walletId in walletIds)
            {
                sb.Append(" or ");
                string walletRowKey = TradeVolumeEntity.ByWallet.GenerateRowKey(walletId);
                string walletRowFilter = TableQuery.GenerateFilterCondition(_rowKey, QueryComparisons.Equal, walletRowKey);
                sb.Append(walletRowFilter);
            }
            sb.Append(")");
            string filter = sb.ToString();
            var query = new TableQuery<TradeVolumeEntity>().Where(filter);
            var items = await storage.WhereAsync(query);
            var itemsDict = items.ToDictionary(i => i.RowKey, i => i);

            var result = new Dictionary<string, double[]>();
            foreach (var userId in userIds)
            {
                string userRowKey = TradeVolumeEntity.ByUser.GenerateRowKey(userId);
                var userTradeVolume = itemsDict.ContainsKey(userRowKey) ? itemsDict[userRowKey] : null;
                double baseVolume = userTradeVolume?.BaseVolume ?? 0;
                double quotingVolume = userTradeVolume?.QuotingVolume ?? 0;
                result.Add(GetUserVolumeKey(userId), new[] { baseVolume, quotingVolume });
            }
            foreach (var walletId in walletIds)
            {
                string walletRowKey = TradeVolumeEntity.ByWallet.GenerateRowKey(walletId);
                var walletTradeVolume = itemsDict.ContainsKey(walletRowKey) ? itemsDict[walletRowKey] : null;
                double baseVolume = walletTradeVolume?.BaseVolume ?? 0;
                double quotingVolume = walletTradeVolume?.QuotingVolume ?? 0;
                result.Add(GetWalletVolumeKey(walletId), new[] { baseVolume, quotingVolume });
            }
            return result;
        }

        public string GetUserVolumeKey(string userId)
        {
            return $"u_{userId}";
        }

        public string GetWalletVolumeKey(string walletId)
        {
            return $"w_{walletId}";
        }

        public async Task<(double, double)> GetPeriodClientVolumeAsync(
            string baseAssetId,
            string quotingAssetId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser)
        {
            if (quotingAssetId == null)
            {
                double result = await GetAssetTradeVolumeAsync(
                    from,
                    to,
                    clientId,
                    baseAssetId,
                    isUser);
                return (result, 0);
            }

            var storage = await GetStorageAsync(baseAssetId, quotingAssetId);
            return await GetTableTradeVolumeAsync(
                from,
                to,
                clientId,
                storage,
                isUser);
        }

        private async Task<(double, double)> GetTableTradeVolumeAsync(
            DateTime from,
            DateTime to,
            string clientId,
            INoSQLTableStorage<TradeVolumeEntity> storage,
            bool isUser)
        {
            string fromFilter = TableQuery.GenerateFilterCondition(
                _partitionKey,
                QueryComparisons.GreaterThanOrEqual,
                TradeVolumeEntity.GeneratePartitionKey(from));
            string toFilter = TableQuery.GenerateFilterCondition(
                _partitionKey,
                QueryComparisons.LessThan,
                TradeVolumeEntity.GeneratePartitionKey(to));
            var filter = TableQuery.CombineFilters(fromFilter, TableOperators.And, toFilter);
            if (clientId != Constants.AllClients)
            {
                var rowKeyFilter = TableQuery.GenerateFilterCondition(
                    _rowKey,
                    QueryComparisons.Equal,
                    isUser
                        ? TradeVolumeEntity.ByUser.GenerateRowKey(clientId)
                        : TradeVolumeEntity.ByWallet.GenerateRowKey(clientId));
                filter = TableQuery.CombineFilters(filter, TableOperators.And, rowKeyFilter);
            }
            var query = new TableQuery<TradeVolumeEntity>().Where(filter);
            var items = await storage.WhereAsync(query, i =>
                clientId != Constants.AllClients || i.RowKey == TradeVolumeEntity.ByUser.GenerateRowKey(i.UserId));
            double baseResult = 0, quotingResult = 0;
            foreach (var item in items)
            {
                baseResult += item.BaseVolume ?? 0;
                quotingResult += item.QuotingVolume ?? 0;
            }
            if (clientId == Constants.AllClients)
            {
                baseResult /= 2;
                quotingResult /= 2;
            }
            return (baseResult, quotingResult);
        }

        private async Task<double> GetAssetTradeVolumeAsync(
            DateTime from,
            DateTime to,
            string clientId,
            string assetId,
            bool isUser)
        {
            var tables = await GetTableNamesAsync(assetId);
            var tradeVolumes = new List<double>();
            const int step = 500;
            for (int i = 0; i < tables.Count; i += step)
            {
                var tablesRange = tables.GetRange(i, Math.Min(step, tables.Count - i));
                await Task.WhenAll(tablesRange.Select(t =>
                    AddTradeVolumeAsync(
                        from,
                        to,
                        clientId,
                        t,
                        tradeVolumes,
                        isUser)));
            }
            return tradeVolumes.Sum();
        }

        private async Task AddTradeVolumeAsync(
            DateTime from,
            DateTime to,
            string clientId,
            string tableName,
            List<double> tradeVolumes,
            bool isUser)
        {
            var storage = AzureTableStorage<TradeVolumeEntity>.Create(_connectionStringManager, tableName, _logFactory, _timeout);
            (double tradeVolume, _) = await GetTableTradeVolumeAsync(
                from,
                to,
                clientId,
                storage,
                isUser);
            if (tradeVolume == 0)
                return;
            await _lock.WaitAsync();
            try
            {
                tradeVolumes.Add(tradeVolume);
            }
            finally
            {
                _lock.Release();
            }
        }

        private async Task<List<string>> GetTableNamesAsync(string assetId)
        {
            string assetName = await _assetsDictionary.GetShortNameAsync(assetId);
            string tablesPrefix = string.Format(Constants.TableNamesPrefix, assetName);
            TableContinuationToken token = null;
            var result = new List<string>();
            do
            {
                var response = await _tableClient.ListTablesSegmentedAsync(tablesPrefix, token);
                token = response.ContinuationToken;
                foreach (var table in response.Results)
                {
                    result.Add(table.Name);
                }
            }
            while (token != null);
            return result;
        }

        private async Task<INoSQLTableStorage<TradeVolumeEntity>> GetStorageAsync(string baseAssetId, string quotingAssetId)
        {
            baseAssetId = await _assetsDictionary.GetShortNameAsync(baseAssetId);
            quotingAssetId = await _assetsDictionary.GetShortNameAsync(quotingAssetId);
            string tableName = string.Format(Constants.TableNameFormat, baseAssetId, quotingAssetId);
            if (_azureTables.TryGetValue(tableName, out var storage))
                return storage;
            var result = AzureTableStorage<TradeVolumeEntity>.Create(_connectionStringManager, tableName, _logFactory, _timeout);
            _azureTables.TryAdd(tableName, result);
            return result;
        }
    }
}
