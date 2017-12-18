using System;
using System.Linq;
using System.Collections.Generic;
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

        public async Task UpdateTradeVolumesForBothAssetsAsync(
            DateTime dateTime,
            string clientId,
            string baseAssetId,
            double baseVolume,
            string quotingAssetId,
            double? quotingVolume)
        {
            var baseEntity = TradeVolumeEntity.Create(
                dateTime,
                clientId,
                baseAssetId,
                baseVolume,
                quotingAssetId,
                quotingVolume);
            var baseStorage = GetStorage(baseAssetId, quotingAssetId);
            await baseStorage.InsertOrReplaceAsync(baseEntity);
        }

        public async Task<double> GetPeriodClientVolumeAsync(
            string baseAssetId,
            string quotingAssetId,
            string clientId,
            DateTime from,
            DateTime to)
        {
            var tradeVolumes = new List<double>();
            if (quotingAssetId == null)
                return await GetAssetTradeVolumeAsync(
                    from,
                    to,
                    clientId,
                    baseAssetId);

            var storage = GetStorage(baseAssetId, quotingAssetId);
            return await GetTableTradeVolumeAsync(
                from,
                to,
                clientId,
                storage);
        }

        public async Task<(double, double)> GetClientPairValuesAsync(
            DateTime date,
            string clientId,
            string baseAssetId,
            string quotingAssetId)
        {
            double baseTradeVolume = await GetTradeVolumeAsync(
                date,
                clientId,
                baseAssetId,
                quotingAssetId);

            double quotingTradeVolume = await GetTradeVolumeAsync(
                date,
                clientId,
                quotingAssetId,
                baseAssetId);

            return (baseTradeVolume, quotingTradeVolume);
        }

        private async Task<double> GetTradeVolumeAsync(
            DateTime date,
            string clientId,
            string baseAssetId,
            string quotingAssetId)
        {
            var storage = GetStorage(baseAssetId, quotingAssetId);
            var result = await storage.GetDataAsync(
                TradeVolumeEntity.GeneratePartitionKey(date),
                TradeVolumeEntity.GenerateRowKey(clientId));
            return result != null ? (result.BaseVolume.HasValue ? result.BaseVolume.Value : 0) : 0;
        }

        private async Task<double> GetTableTradeVolumeAsync(
            DateTime from,
            DateTime to,
            string clientId,
            INoSQLTableStorage<TradeVolumeEntity> storage)
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
                    TradeVolumeEntity.GenerateRowKey(clientId));
                filter = TableQuery.CombineFilters(filter, TableOperators.And, rowKeyFilter);
            }
            var query = new TableQuery<TradeVolumeEntity>().Where(filter);
            var items = await storage.WhereAsync(query);
            double result = items.Sum(i => i.BaseVolume.HasValue ? i.BaseVolume.Value : 0);
            if (clientId == Constants.AllClients)
                result /= 2;
            return result;
        }

        private async Task<double> GetAssetTradeVolumeAsync(
            DateTime from,
            DateTime to,
            string clientId,
            string assetId)
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
                        tradeVolumes)));
            }
            return tradeVolumes.Sum();
        }

        private async Task AddTradeVolumeAsync(
            DateTime from,
            DateTime to,
            string clientId,
            string tableName,
            List<double> tradeVolumes)
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
                storage);
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
            assetId = assetId.Replace("-", "");
            string tablesPrefix = string.Format(Constants.TableNamesPrefix, assetId);
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

        private INoSQLTableStorage<TradeVolumeEntity> GetStorage(string baseAssetId, string quotingAssetId)
        {
            baseAssetId = baseAssetId.Replace("-", "");
            quotingAssetId = quotingAssetId.Replace("-", "");
            string tableName = string.Format(Constants.TableNameFormat, baseAssetId, quotingAssetId);
            return AzureTableStorage<TradeVolumeEntity>.Create(
                _connectionStringManager,
                tableName,
                _log,
                _timeout);
        }
    }
}
