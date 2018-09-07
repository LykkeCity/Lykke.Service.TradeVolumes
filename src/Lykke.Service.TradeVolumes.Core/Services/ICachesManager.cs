using System;
using System.Threading.Tasks;

namespace Lykke.Service.TradeVolumes.Core.Services
{
    public interface ICachesManager
    {
        Task<double?> GetAssetTradeVolumeAsync(
            string clientId,
            string assetId,
            DateTime from,
            DateTime to);
        Task AddAssetTradeVolumeAsync(
            string clientId,
            string assetId,
            DateTime time,
            double tradeVolume);
        Task<(double?, double?)> GetAssetPairTradeVolumeAsync(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to);
        Task AddAssetPairTradeVolumeAsync(
            string clientId,
            string assetPairId,
            DateTime time,
            (double, double) tradeVolumes);
    }
}
