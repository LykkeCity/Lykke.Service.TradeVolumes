namespace Lykke.Service.TradeVolumes.Models
{
    public class AssetTradeVolumeResponse
    {
        public string ClientId { get; set; }

        public string AssetId { get; set; }

        public double Volume { get; set; }
    }
}
