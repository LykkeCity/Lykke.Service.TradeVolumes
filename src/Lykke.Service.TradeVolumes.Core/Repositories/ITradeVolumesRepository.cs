using System;
using System.Threading.Tasks;

namespace Lykke.Service.TradeVolumes.Core.Repositories
{
    public interface ITradeVolumesRepository
    {
        Task NotThreadSafeTradeVolumesUpdateAsync(
            DateTime dateTime,
            string userId,
            string walletId,
            string baseAssetId,
            double baseVolume,
            string quotingAssetId,
            double? quotingVolume,
            bool isUser);
        Task<double> GetPeriodClientVolumeAsync(
            string baseAssetId,
            string quotingAssetId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser);
        Task<(double, double)> GetClientPairValuesAsync(
            DateTime date,
            string clientId,
            string baseAssetId,
            string quotingAssetId,
            bool isUser);
    }
}
