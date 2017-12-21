using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Service.TradeVolumes.AzureRepositories.Models
{
    public class TradeVolumeEntity : TableEntity
    {
        private const string _dateTimeFormat = "yyyyMMddHH";

        public string UserId { get; set; }

        public string WalletId { get; set; }

        public string BaseAssetId { get; set; }

        public double? BaseVolume { get; set; }

        public string QuotingAssetId { get; set; }

        public double? QuotingVolume { get; set; }

        public DateTime DateTime { get; set; }

        public class ByUser
        {
            public static TradeVolumeEntity Create(
                DateTime datetime,
                string userId,
                string walletId,
                string baseAssetId,
                double? baseVolume,
                string quotingAssetId,
                double? quotingVolume)
            {
                return new TradeVolumeEntity
                {
                    PartitionKey = GeneratePartitionKey(datetime),
                    RowKey = GenerateRowKey(userId),
                    UserId = userId,
                    WalletId = walletId,
                    BaseAssetId = baseAssetId,
                    BaseVolume = baseVolume,
                    QuotingAssetId = quotingAssetId,
                    QuotingVolume = quotingVolume,
                    DateTime = datetime,
                };
            }

            public static string GenerateRowKey(string userId)
            {
                return userId;
            }
        }

        public class ByWallet
        {
            public static TradeVolumeEntity Create(
                DateTime datetime,
                string userId,
                string walletId,
                string baseAssetId,
                double? baseVolume,
                string quotingAssetId,
                double? quotingVolume)
            {
                return new TradeVolumeEntity
                {
                    PartitionKey = GeneratePartitionKey(datetime),
                    RowKey = GenerateRowKey(walletId),
                    UserId = userId,
                    WalletId = walletId,
                    BaseAssetId = baseAssetId,
                    BaseVolume = baseVolume,
                    QuotingAssetId = quotingAssetId,
                    QuotingVolume = quotingVolume,
                    DateTime = datetime,
                };
            }

            public static string GenerateRowKey(string walletId)
            {
                return walletId;
            }
        }

        public static string GeneratePartitionKey(DateTime datetime)
        {
            return datetime.ToString(_dateTimeFormat);
        }

        public static string GenerateRowKey(string id)
        {
            return id;
        }
    }
}
