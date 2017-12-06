using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Service.TradeVolumes.AzureRepositories.Models
{
    public class TradeVolumeEntity : TableEntity
    {
        public string BaseAssetId { get; set; }

        public double? BaseVolume { get; set; }

        public string QuotingAssetId { get; set; }

        public double? QuotingVolume { get; set; }

        public DateTime DateTime { get; set; }

        public static TradeVolumeEntity Create(
            string clientId,
            string baseAssetId,
            double? baseVolume,
            string quotingAssetId,
            double? quotingVolume,
            DateTime datetime)
        {
            return new TradeVolumeEntity
            {
                PartitionKey = GeneratePartitionKey(clientId),
                RowKey = GenerateRowKey(quotingAssetId),
                BaseAssetId = baseAssetId,
                BaseVolume = baseVolume,
                QuotingAssetId = quotingAssetId,
                QuotingVolume = quotingVolume,
                DateTime = datetime,
            };
        }

        public static string GeneratePartitionKey(string clientId)
        {
            return clientId;
        }

        public static string GenerateRowKey(string quotingAssetId)
        {
            return quotingAssetId;
        }
    }
}
