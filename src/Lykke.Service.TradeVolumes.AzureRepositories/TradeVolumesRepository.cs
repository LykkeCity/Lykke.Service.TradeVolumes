using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Common.Log;
using AzureStorage;
using AzureStorage.Tables;
using Lykke.SettingsReader;
using Lykke.Service.TradeVolumes.Core;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Core.Repositories;
using Lykke.Service.TradeVolumes.AzureRepositories.Models;

namespace Lykke.Service.TradeVolumes.AzureRepositories
{
    public class TradeVolumesRepository : ITradeVolumesRepository
    {
        private const string _partitionKey = "PartitionKey";
        private const string _rowKey = "RowKey";

        private readonly IReloadingManager<string> _connectionStringManager;
        private readonly IAssetsDictionary _assetsDictionary;
        private readonly ILog _log;
        private readonly CloudTableClient _tableClient;
        private readonly TimeSpan _timeout = TimeSpan.FromMinutes(5);
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        public TradeVolumesRepository(
            IReloadingManager<string> connectionStringManager,
            IAssetsDictionary assetsDictionary,
            ILog log)
        {
            _connectionStringManager = connectionStringManager;
            _assetsDictionary = assetsDictionary;
            _log = log;
            _tableClient = CloudStorageAccount.Parse(connectionStringManager.CurrentValue).CreateCloudTableClient();
        }

        public async Task NotThreadSafeTradeVolumesUpdateAsync(
            DateTime dateTime,
            string baseAssetId,
            string quotingAssetId,
            Dictionary<string, (string, double[])> userWalletsData)
        {
            var items = new List<TradeVolumeEntity>(userWalletsData.Count * 2);
            foreach (var userWalletData in userWalletsData)
            {
                string userId = userWalletData.Value.Item1;
                string walletId = userWalletData.Key;
                var userWalletTradeVolumes = userWalletData.Value.Item2;
                items.Add(
                    TradeVolumeEntity.ByUser.Create(
                        dateTime,
                        userId,
                        walletId,
                        baseAssetId,
                        userWalletTradeVolumes[0],
                        quotingAssetId,
                        userWalletTradeVolumes[1]));
                items.Add(
                    TradeVolumeEntity.ByWallet.Create(
                        dateTime,
                        userId,
                        walletId,
                        baseAssetId,
                        userWalletTradeVolumes[2],
                        quotingAssetId,
                        userWalletTradeVolumes[3]));
            }
            var baseStorage = await GetStorageAsync(baseAssetId, quotingAssetId);
            await baseStorage.InsertOrReplaceAsync(items);
        }

        public async Task<Dictionary<string, (string, double[])>> GetUserWalletsTradeVolumesAsync(
            DateTime date,
            IEnumerable<(string, string)> userWallets,
            string baseAssetId,
            string quotingAssetId)
        {
            var baseWalletsData = await GetTradeVolumeAsync(
                date,
                userWallets,
                baseAssetId,
                quotingAssetId);

            var quotingWalletData = await GetTradeVolumeAsync(
                date,
                userWallets,
                quotingAssetId,
                baseAssetId);

            var result = new Dictionary<string, (string, double[])>();
            foreach (var userWalletInfo in userWallets)
            {
                string walletId = userWalletInfo.Item2;
                var baseData = baseWalletsData[walletId];
                var quotingData = quotingWalletData[walletId];
                result.Add(walletId, (userWalletInfo.Item1, new double[] { baseData.Item1, quotingData.Item1, baseData.Item2, quotingData.Item2 }));
            }

            return result;
        }

        public async Task<double> GetPeriodClientVolumeAsync(
            string baseAssetId,
            string quotingAssetId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser)
        {
            var tradeVolumes = new List<double>();
            if (quotingAssetId == null)
                return await GetAssetTradeVolumeAsync(
                    from,
                    to,
                    clientId,
                    baseAssetId,
                    isUser);

            var storage = await GetStorageAsync(baseAssetId, quotingAssetId);
            return await GetTableTradeVolumeAsync(
                from,
                to,
                clientId,
                storage,
                isUser);
        }

        private async Task<Dictionary<string, (double, double)>> GetTradeVolumeAsync(
            DateTime date,
            IEnumerable<(string, string)> userWallets,
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
            foreach (var userWallet in userWallets)
            {
                if (isFirstRowFilter)
                    isFirstRowFilter = false;
                else
                    sb.Append(" or ");
                string userRowKey = TradeVolumeEntity.ByUser.GenerateRowKey(userWallet.Item1);
                string userRowFilter = TableQuery.GenerateFilterCondition(_rowKey, QueryComparisons.Equal, userRowKey);
                string walletRowKey = TradeVolumeEntity.ByWallet.GenerateRowKey(userWallet.Item2);
                string walletRowFilter = TableQuery.GenerateFilterCondition(_rowKey, QueryComparisons.Equal, walletRowKey);
                sb.Append($"{userRowFilter} or {walletRowFilter}");
            }
            sb.Append(")");
            string filter = sb.ToString();
            var query = new TableQuery<TradeVolumeEntity>().Where(filter);
            var items = await storage.WhereAsync(query);
            var result = new Dictionary<string, (double, double)>();
            foreach (var userWallet in userWallets)
            {
                string userRowKey = TradeVolumeEntity.ByUser.GenerateRowKey(userWallet.Item1);
                string walletRowKey = TradeVolumeEntity.ByWallet.GenerateRowKey(userWallet.Item2);
                var userTradeVolume = items.FirstOrDefault(i => i.RowKey == userRowKey);
                double userVolume = userTradeVolume != null && userTradeVolume.BaseVolume.HasValue
                    ? userTradeVolume.BaseVolume.Value : 0;
                var walletTradeVolume = items.FirstOrDefault(i => i.RowKey == walletRowKey);
                double walletVolume = walletTradeVolume != null && walletTradeVolume.BaseVolume.HasValue
                    ? walletTradeVolume.BaseVolume.Value : 0;
                result.Add(userWallet.Item2, (userVolume, walletVolume));
            }
            return result;
        }

        private async Task<double> GetTableTradeVolumeAsync(
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
            double result = items.Sum(i => i.BaseVolume.HasValue ? i.BaseVolume.Value : 0);
            if (clientId == Constants.AllClients)
                result /= 2;
            return result;
        }

        private async Task<double> GetAssetTradeVolumeAsync(
            DateTime from,
            DateTime to,
            string clientId,
            string assetId,
            bool isUser)
        {
            var tables = await GetTableNamesAsync(assetId);
            var possibleTableNames = await _assetsDictionary.GeneratePossibleTableNamesAsync(assetId);
            var tablesToProcess = new List<string>(tables.Count);
            foreach (var possibleTableName in possibleTableNames)
            {
                if (tables.Contains(possibleTableName))
                    tablesToProcess.Add(possibleTableName);
            }
            var tradeVolumes = new List<double>();
            const int step = 500;
            for (int i = 0; i < tablesToProcess.Count; i += step)
            {
                var tablesRange = tablesToProcess.GetRange(i, Math.Min(step, tablesToProcess.Count - i));
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
            var storage = AzureTableStorage<TradeVolumeEntity>.Create(
                _connectionStringManager,
                tableName,
                _log,
                _timeout);
            double tradeVolume = await GetTableTradeVolumeAsync(
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

        private async Task<HashSet<string>> GetTableNamesAsync(string assetId)
        {
            string assetName = await _assetsDictionary.GetShortNameAsync(assetId);
            string tablesPrefix = string.Format(Constants.TableNamesPrefix, assetName);
            TableContinuationToken token = null;
            var result = new HashSet<string>();
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
            return AzureTableStorage<TradeVolumeEntity>.Create(
                _connectionStringManager,
                tableName,
                _log,
                _timeout);
        }
    }
}
