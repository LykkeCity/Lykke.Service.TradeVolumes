using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Common.Log;
using AzureStorage;
using AzureStorage.Tables;
using Lykke.SettingsReader;
using Lykke.Service.TradeVolumes.Core;
using Lykke.Service.TradeVolumes.Core.Repositories;
using Lykke.Service.TradeVolumes.AzureRepositories.Models;

namespace Lykke.Service.TradeVolumes.AzureRepositories
{
    public class TradeVolumesRepository : ITradeVolumesRepository
    {
        private const string _partitionKey = "PartitionKey";
        private const string _rowKey = "RowKey";

        private readonly IReloadingManager<string> _connectionStringManager;
        private readonly ILog _log;
        private readonly CloudTableClient _tableClient;
        private readonly TimeSpan _timeout = TimeSpan.FromMinutes(5);
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private readonly ConcurrentDictionary<string, INoSQLTableStorage<TradeVolumeEntity>> _storagesCache =
            new ConcurrentDictionary<string, INoSQLTableStorage<TradeVolumeEntity>>();
        private DateTime _cacheDate = DateTime.MinValue;

        public TradeVolumesRepository(IReloadingManager<string> connectionStringManager, ILog log)
        {
            _connectionStringManager = connectionStringManager;
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
            if (dateTime.Subtract(_cacheDate).TotalHours > 1)
            {
                _storagesCache.Clear();
                _cacheDate = dateTime;
            }

            var baseEntity = TradeVolumeEntity.Create(
                clientId,
                baseAssetId,
                baseVolume,
                quotingAssetId,
                quotingVolume,
                dateTime);
            var baseStorage = GetStorage(baseAssetId, dateTime);
            await baseStorage.InsertOrMergeAsync(baseEntity);
        }

        public async Task<double> GetPeriodClientVolumeAsync(
            string baseAssetId,
            string quotingAssetId,
            string clientId,
            DateTime from,
            DateTime to,
            string excludeClientId)
        {
            var tradeVolumes = new List<double>();
            var months = new List<DateTime>();
            var existingTables = new HashSet<string>();
            for (DateTime start = from; start < to; start = start.AddMonths(1))
            {
                months.Add(start);
                await AddExistingTableNamesAsync(baseAssetId, start, existingTables);
            }
            var dates = new List<DateTime>();
            for (DateTime start = from; start < to; start = start.AddHours(1))
            {
                var tableName = GetTableName(baseAssetId, start);
                if (existingTables.Contains(tableName))
                    dates.Add(start);
            }
            await Task.WhenAll(dates.Select(d =>
                AddTradeVolumeAsync(
                    d,
                    clientId,
                    baseAssetId,
                    quotingAssetId,
                    excludeClientId,
                    tradeVolumes)));
            return tradeVolumes.Sum();
        }

        public async Task<(double, double)> GetClientPairValuesAsync(
            DateTime date,
            string clientId,
            string baseAssetId,
            string quotingAssetId,
            string excludeClientId)
        {
            double baseTradeVolume = await GetTradeVolumeAsync(
                date,
                clientId,
                baseAssetId,
                quotingAssetId,
                excludeClientId);

            double quotingTradeVolume = await GetTradeVolumeAsync(
                date,
                clientId,
                quotingAssetId,
                baseAssetId,
                excludeClientId);

            return (baseTradeVolume, quotingTradeVolume);
        }

        private async Task AddTradeVolumeAsync(
            DateTime date,
            string clientId,
            string baseAssetId,
            string quotingAssetId,
            string excludeClientId,
            List<double> tradeVolumes)
        {
            double tradeVolume = await GetTradeVolumeAsync(
                date,
                clientId,
                baseAssetId,
                quotingAssetId,
                excludeClientId);
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

        private async Task<double> GetTradeVolumeAsync(
            DateTime date,
            string clientId,
            string baseAssetId,
            string quotingAssetId,
            string excludeClientId)
        {
            var storage = GetStorage(baseAssetId, date);
            if (clientId == Constants.AllClients)
            {
                string filterAsset = quotingAssetId ?? Constants.AllAssets;
                var data = await storage.GetDataAsync(Constants.AllClients, filterAsset);
                if (data != null)
                    return data.BaseVolume.HasValue ? data.BaseVolume.Value : 0;
            }
            string filter =
                clientId == Constants.AllClients
                ? TableQuery.GenerateFilterCondition(_partitionKey, QueryComparisons.NotEqual, Constants.AllClients)
                : TableQuery.GenerateFilterCondition(_partitionKey, QueryComparisons.Equal, clientId);
            if (!string.IsNullOrWhiteSpace(quotingAssetId))
            {
                var rowFilter = TableQuery.GenerateFilterCondition(_rowKey, QueryComparisons.Equal, quotingAssetId);
                filter = TableQuery.CombineFilters(filter, TableOperators.And, rowFilter);
            }
            var query = new TableQuery<TradeVolumeEntity>().Where(filter);
            var items = await storage.WhereAsync(query);
            if (clientId != excludeClientId && excludeClientId != null)
                items = items.Where(i => i.PartitionKey != excludeClientId);
            double result = items.Sum(i => i.BaseVolume.HasValue ? i.BaseVolume.Value : 0);
            if (clientId == Constants.AllClients)
            {
                result /= 2;
                var now = DateTime.UtcNow;
                if (now.Subtract(date).TotalHours >= 1 || now.Hour != date.Hour)
                {
                    double quotingVolume = items.Sum(i => i.QuotingVolume.HasValue ? i.QuotingVolume.Value : 0);
                    ThreadPool.QueueUserWorkItem(_ =>
                        AddAllVolumeAsync(
                            storage,
                            baseAssetId,
                            result,
                            quotingAssetId,
                            quotingVolume,
                            now));
                }
            }
            return result;
        }

        private INoSQLTableStorage<TradeVolumeEntity> GetStorage(string assetId, DateTime date)
        {
            string tableName = GetTableName(assetId, date);
            if (!_storagesCache.TryGetValue(tableName, out INoSQLTableStorage<TradeVolumeEntity> storage))
            {
                storage = AzureTableStorage<TradeVolumeEntity>.Create(_connectionStringManager, tableName, _log, _timeout);
                _storagesCache.TryAdd(tableName, storage);
            }
            return storage;
        }

        private static string GetTableName(string assetId, DateTime date)
        {
            assetId = assetId.Replace("-", "");
            string tableName = $"Asset{assetId}on{date.ToString(Constants.DateTimeFormat)}";
            return tableName;
        }

        private static string GetTablesPrefix(string assetId, DateTime date)
        {
            assetId = assetId.Replace("-", "");
            string tableName = $"Asset{assetId}on{date.ToString("yyyyMM")}";
            return tableName;
        }

        private async Task AddExistingTableNamesAsync(string assetId, DateTime date, ICollection<string> existingTables)
        {
            string tablesPrefix = GetTablesPrefix(assetId, date);
            TableContinuationToken token = null;
            do
            {
                var response = await _tableClient.ListTablesSegmentedAsync(tablesPrefix, token);
                token = response.ContinuationToken;
                foreach (var table in response.Results)
                {
                    existingTables.Add(table.Name);
                }
            }
            while (token != null);
        }

        private async void AddAllVolumeAsync(
            INoSQLTableStorage<TradeVolumeEntity> storage,
            string baseAssetId,
            double baseVolume,
            string quotingAssetId,
            double quotingVolume,
            DateTime dateTime)
        {
            var baseEntity = TradeVolumeEntity.Create(
                Constants.AllClients,
                baseAssetId,
                baseVolume,
                quotingAssetId ?? Constants.AllAssets,
                quotingVolume,
                dateTime);
            await storage.InsertOrMergeAsync(baseEntity);
        }
    }
}
