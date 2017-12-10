using System;
using System.Linq;
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
            var baseEntity = TradeVolumeEntity.Create(
                clientId,
                baseAssetId,
                baseVolume,
                quotingAssetId,
                quotingVolume,
                dateTime);
            var baseStorage = GetStorage(baseAssetId, dateTime);
            await baseStorage.InsertOrMergeAsync(baseEntity);

            var quotingEntity = TradeVolumeEntity.Create(
                clientId,
                quotingAssetId,
                quotingVolume,
                baseAssetId,
                baseVolume,
                dateTime);
            var quotingStorage = GetStorage(quotingAssetId, dateTime);
            await quotingStorage.InsertOrMergeAsync(quotingEntity);
        }

        public async Task<double> GetPeriodClientVolumeAsync(
            string baseAssetId,
            string quotingAssetId,
            string clientId,
            DateTime from,
            DateTime to,
            string excludeClientId)
        {
            double result = 0;
            for (DateTime start = from.Date; start < to; start = start.AddHours(1))
            {
                double tradeVolume =  await GetTradeVolumeAsync(
                    start,
                    clientId,
                    baseAssetId,
                    quotingAssetId,
                    excludeClientId);
                result += tradeVolume;
            }
            return result;
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

        private async Task<double> GetTradeVolumeAsync(
            DateTime date,
            string clientId,
            string baseAssetId,
            string quotingAssetId,
            string excludeClientId)
        {
            var storage = await GetStorageAsync(baseAssetId, date);
            if (storage == null)
                return 0;
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
            double result = items
                .Where(i => i.PartitionKey != excludeClientId)
                .Sum(i => i.BaseVolume.HasValue ? i.BaseVolume.Value : 0);
            return clientId == Constants.AllClients
                ? result / 2
                : result;
        }

        private INoSQLTableStorage<TradeVolumeEntity> GetStorage(string assetId, DateTime date)
        {
            string tableName = GetTableName(assetId, date);
            return AzureTableStorage<TradeVolumeEntity>.Create(_connectionStringManager, tableName, _log);
        }

        private async Task<INoSQLTableStorage<TradeVolumeEntity>> GetStorageAsync(string assetId, DateTime date)
        {
            string tableName = GetTableName(assetId, date);
            var tableRef = _tableClient.GetTableReference(tableName);
            bool tableExists = await tableRef.ExistsAsync();
            if (!tableExists)
                return null;
            return AzureTableStorage<TradeVolumeEntity>.Create(_connectionStringManager, tableName, _log);
        }

        private static string GetTableName(string assetId, DateTime date)
        {
            assetId = assetId.Replace("-", "");
            string tableName = $"Asset{assetId}on{date.ToString(Constants.DateTimeFormat)}";
            return tableName;
        }
    }
}
