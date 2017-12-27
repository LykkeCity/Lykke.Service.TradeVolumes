using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Job.TradesConverter.Contract;

namespace Lykke.Service.TradeVolumes.Core.Services
{
    public interface ITradeVolumesCalculator
    {
        Task AddTradeLogItemsAsync(List<TradeLogItem> items);

        Task<double> GetPeriodAssetVolumeAsync(
            string assetId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser);

        Task<(double, double)> GetPeriodAssetPairVolumeAsync(
            string assetPairId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser);
    }
}
