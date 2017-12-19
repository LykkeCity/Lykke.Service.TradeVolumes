namespace Lykke.Service.TradeVolumes.Client.Models
{
    /// <summary>
    /// TradeVolume response for asset.
    /// </summary>
    public class AssetTradeVolumeResponse
    {
        /// <summary>Client id.</summary>
        public string ClientId { get; set; }

        /// <summary>Asset id.</summary>
        public string AssetId { get; set; }

        /// <summary>Volume.</summary>
        public double Volume { get; set; }
    }
}
