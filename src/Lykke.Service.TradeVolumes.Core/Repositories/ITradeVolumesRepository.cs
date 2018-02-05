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
            ICollection<(string, string, double, double)> userVolumes,
            ICollection<(string, string, double, double)> waletVolumes);
        Task<Dictionary<string, double[]>> GetUserWalletsTradeVolumesAsync(
            DateTime date,
            IEnumerable<(string, string)> userWallets,
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
