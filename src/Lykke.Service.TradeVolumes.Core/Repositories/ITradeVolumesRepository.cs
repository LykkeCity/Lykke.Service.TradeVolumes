using System;
using System.Threading.Tasks;

namespace Lykke.Service.TradeVolumes.Core.Repositories
{
    public interface ITradeVolumesRepository
    {
        Task UpdateTradeVolumesAsync(
            DateTime dateTime,
            string clientId,
            string baseAssetId,
            double baseVolume,
            string quotingAssetId,
            double? quotingVolume);
        Task<double> GetPeriodClientVolumeAsync(
            string baseAssetId,
            string quotingAssetId,
            string clientId,
            DateTime from,
            DateTime to,
            string excludeClientId);
        Task<(double, double)> GetClientPairValuesAsync(
            DateTime date,
            string clientId,
            string baseAssetId,
            string quotingAssetId,
            string excludeClientId);
    }
}
