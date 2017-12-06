using System;
using System.Threading.Tasks;
using Lykke.Service.TradeVolumes.Core.Messages;

namespace Lykke.Service.TradeVolumes.Core.Services
{
    public interface ITradeVolumesCalculator
    {
        Task AddTradeLogItemAsync(TradeLogItem item);

        Task<double> GetPeriodAssetVolumeAsync(
            string assetId,
            string clientId,
            DateTime from,
            DateTime to);

        Task<(double, double)> GetPeriodAssetPairVolumeAsync(
            string assetPairId,
            string clientId,
            DateTime from,
            DateTime to);
    }
}
