using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Common.Log;
using AzureStorage;
using AzureStorage.Tables;
using Lykke.SettingsReader;
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

        public TradeVolumesRepository(IReloadingManager<string> connectionStringManager, ILog log)
        {
            _connectionStringManager = connectionStringManager;
            _log = log;
        }

        public async Task UpdateTradeVolumesAsync(
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
            DateTime to)
        {
            double result = 0;
            for (DateTime start = from.Date; start < to.Date; start = start.AddDays(1))
            {
                double tradeVolume =  await GetTradeVolumeAsync(
                    start,
                    clientId,
                    baseAssetId,
                    quotingAssetId);
                result += tradeVolume;
            }
            return result;
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
            var storage = GetStorage(baseAssetId, date);
            string filter = TableQuery.GenerateFilterCondition(_partitionKey, QueryComparisons.Equal, clientId);
            if (!string.IsNullOrWhiteSpace(quotingAssetId))
            {
                var rowFilter = TableQuery.GenerateFilterCondition(_rowKey, QueryComparisons.Equal, quotingAssetId);
                filter = TableQuery.CombineFilters(filter, TableOperators.And, rowFilter);
            }
            var query = new TableQuery<TradeVolumeEntity>().Where(filter);
            var items = await storage.WhereAsync(query);
            return items.Sum(i => i.BaseVolume.HasValue ? i.BaseVolume.Value : 0);
        }

        private INoSQLTableStorage<TradeVolumeEntity> GetStorage(string assetId, DateTime date)
        {
            string tableName = $"{assetId}{date.ToString("yyyyMMdd")}";
            return AzureTableStorage<TradeVolumeEntity>.Create(_connectionStringManager, tableName, _log);
        }
    }
}
