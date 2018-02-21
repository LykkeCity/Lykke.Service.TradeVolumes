using System.Threading.Tasks;

namespace Lykke.Service.TradeVolumes.Core.Services
{
    public interface IAssetsDictionary
    {
        Task<(string, string)> GetAssetIdsAsync(string assetPair);
        Task<string> GetShortNameAsync(string assetId);
        Task<string> GetAssetPairIdAsync(string asset1, string asset2);
    }
}
