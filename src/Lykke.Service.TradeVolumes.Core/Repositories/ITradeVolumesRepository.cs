using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Service.TradeVolumes.Core.Repositories
{
    public interface ITradeVolumesRepository
    {
        Task NotThreadSafeTradeVolumesUpdateAsync(
            DateTime dateTime,
            string baseAssetId,
            string quotingAssetId,
            ICollection<(string, double, double)> userVolumes,
            ICollection<(string, string, double, double)> waletVolumes);
        Task<Dictionary<string, double[]>> GetUserWalletsTradeVolumesAsync(
            DateTime date,
            IEnumerable<string> userIds,
            IEnumerable<string> walletIds,
            string baseAssetId,
            string quotingAssetId);
        Task<(double, double)> GetPeriodClientVolumeAsync(
            string baseAssetId,
            string quotingAssetId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser);
    }
}
