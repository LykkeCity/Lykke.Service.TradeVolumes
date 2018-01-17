namespace Lykke.Service.TradeVolumes.Models
{
    public class AssetPairTradeVolumeResponse
    {
        public string ClientId { get; set; }

        public string WalletId { get; set; }

        public string AssetPairId { get; set; }

        public double BaseVolume { get; set; }

        public double QuotingVolume { get; set; }
    }
}
