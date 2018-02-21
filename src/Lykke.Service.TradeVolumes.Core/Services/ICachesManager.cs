using System;

namespace Lykke.Service.TradeVolumes.Core.Services
{
    public interface ICachesManager
    {
        bool TryGetAssetTradeVolume(
            string clientId,
            string assetId,
            DateTime from,
            DateTime to,
            out double result);
        void AddAssetTradeVolume(
            string clientId,
            string assetId,
            DateTime from,
            DateTime to,
            double tradeVolume);
        void UpdateAssetTradeVolume(
            string clientId,
            string assetId,
            DateTime time,
            double tradeVolume);
        bool TryGetAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to,
            out (double, double) result);
        void AddAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to,
            (double, double) tradeVolumes);
        void UpdateAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime time,
            (double, double) tradeVolumes);
    }
}
