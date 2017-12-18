using System;
using Microsoft.WindowsAzure.Storage.Table;
using Lykke.Service.TradeVolumes.Core;

namespace Lykke.Service.TradeVolumes.AzureRepositories.Models
{
    public class TradeVolumeEntity : TableEntity
    {
        public string ClientId { get; set; }

        public string BaseAssetId { get; set; }

        public double? BaseVolume { get; set; }

        public string QuotingAssetId { get; set; }

        public double? QuotingVolume { get; set; }

        public DateTime DateTime { get; set; }

        public static TradeVolumeEntity Create(
            DateTime datetime,
            string clientId,
            string baseAssetId,
            double? baseVolume,
            string quotingAssetId,
            double? quotingVolume)
        {
            return new TradeVolumeEntity
            {
                PartitionKey = GeneratePartitionKey(datetime),
                RowKey = GenerateRowKey(clientId),
                ClientId = clientId,
                BaseAssetId = baseAssetId,
                BaseVolume = baseVolume,
                QuotingAssetId = quotingAssetId,
                QuotingVolume = quotingVolume,
                DateTime = datetime,
            };
        }

        public static string GeneratePartitionKey(DateTime datetime)
        {
            return datetime.ToString(Constants.DateTimeFormat);
        }

        public static string GenerateRowKey(string clientId)
        {
            return clientId;
        }
    }
}
