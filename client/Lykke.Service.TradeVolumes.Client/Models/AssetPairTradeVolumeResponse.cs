namespace Lykke.Service.TradeVolumes.Client.Models
{
    /// <summary>
    /// TradeVolume response for asset pair.
    /// </summary>
    public class AssetPairTradeVolumeResponse
    {
        /// <summary>Client id.</summary>
        public string ClientId { get; set; }

        /// <summary>Asset pair id.</summary>
        public string AssetPairId { get; set; }

        /// <summary>Base volume.</summary>
        public double BaseVolume { get; set; }

        /// <summary>Quoting volume.</summary>
        public double QuotingVolume { get; set; }
    }
}
