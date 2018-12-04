using System;
using System.Threading.Tasks;

namespace Lykke.Service.TradeVolumes.Core.Services
{
    public interface ICachesManager
    {
        Task<(double?, double?)> GetAssetPairTradeVolumeAsync(
            string assetPairId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser);
        Task AddAssetPairTradeVolumeAsync(
            string assetPairId,
            string assetId,
            string userId,
            string walletId,
            string tradeId,
            DateTime time,
            (double, double) tradeVolumes);
        Task<DateTime> GetFirstCachedTimestampAsync(
            string assetPairId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser);
    }
}
