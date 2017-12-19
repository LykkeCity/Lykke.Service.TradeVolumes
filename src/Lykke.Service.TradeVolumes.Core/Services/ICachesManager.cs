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
            DateTime now,
            out double result);
        void AddAssetTradeVolume(
            string clientId,
            string assetId,
            DateTime from,
            DateTime to,
            DateTime now,
            double tradeVolume);
        bool TryGetAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to,
            DateTime now,
            out (double, double) result);
        void AddAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to,
            DateTime now,
            (double, double) tradeVolumes);
    }
}
