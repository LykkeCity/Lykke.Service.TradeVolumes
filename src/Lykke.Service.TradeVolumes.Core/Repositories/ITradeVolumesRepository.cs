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
            string quotingAssetId,
            double baseUserVolume,
            double quotingUserVolume,
            double baseWalletVolume,
            double quotingWalletVolume);
        Task<double> GetPeriodClientVolumeAsync(
            string baseAssetId,
            string quotingAssetId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser);
        Task<(double, double, double, double)> GetClientPairValuesAsync(
            DateTime date,
            string clientId,
            string walletId,
            string baseAssetId,
            string quotingAssetId);
    }
}
