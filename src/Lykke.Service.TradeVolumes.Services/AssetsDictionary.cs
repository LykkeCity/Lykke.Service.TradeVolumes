using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Lykke.Service.Assets.Client;
using Lykke.Service.Assets.Client.Models;
using Lykke.Service.TradeVolumes.Core;
using Lykke.Service.TradeVolumes.Core.Services;

namespace Lykke.Service.TradeVolumes.Services
{
    public class AssetsDictionary : IAssetsDictionary
    {
        private readonly IAssetsService _assetsService;
        private readonly ConcurrentDictionary<string, AssetPair> _pairsDict = new ConcurrentDictionary<string, AssetPair>();
        private readonly ConcurrentDictionary<string, string> _assetsDict = new ConcurrentDictionary<string, string>();

        public AssetsDictionary(IAssetsService assetsService)
        {
            _assetsService = assetsService;
        }

        public async Task<(string, string)> GetAssetIdsAsync(string assetPair)
        {
            if (_pairsDict.ContainsKey(assetPair))
                return (_pairsDict[assetPair].BaseAssetId, _pairsDict[assetPair].QuotingAssetId);

            var pair = await _assetsService.AssetPairGetAsync(assetPair);
            if (pair == null)
                throw new UnknownPairException($"Unknown assetPair {assetPair}!");

            _pairsDict.TryAdd(pair.Id, pair);

            return (pair.BaseAssetId, pair.QuotingAssetId);
        }

        public async Task<string> GetShortNameAsync(string assetId)
        {
            var alias = CleanupNameForTable(assetId);
            if (alias.Length <= 31) // not GUID
                return alias;

            if (!_assetsDict.ContainsKey(assetId))
            {
                var asset = await _assetsService.AssetGetAsync(assetId);
                if (asset == null)
                    throw new UnknownAssetException($"Unknown asset {assetId}");
                var assetName = string.IsNullOrWhiteSpace(asset.DisplayId) ? asset.Name : asset.DisplayId;
                _assetsDict.TryAdd(assetId, CleanupNameForTable(assetName));
            }

            _assetsDict.TryGetValue(assetId, out string result);
            return result;
        }

        public async Task<string> GetAssetPairIdAsync(string asset1, string asset2)
        {
            string result = await SearchForPairIdAsync(asset1, asset2);
            if (result != null)
                return result;

            var info1 = await _assetsService.AssetGetAsync(asset1);
            var info2 = await _assetsService.AssetGetAsync(asset2);

            var items1 = new HashSet<string> { info1.Name, info1.DisplayId, info1.Symbol };
            var items2 = new HashSet<string> { info2.Name, info2.DisplayId, info2.Symbol };
            foreach (var item1 in items1)
            {
                if (string.IsNullOrWhiteSpace(item1))
                    continue;

                foreach (var item2 in items2)
                {
                    if (string.IsNullOrWhiteSpace(item2))
                        continue;

                    result = await SearchForPairIdAsync(item1, item2);
                    if (result != null)
                        return result;
                }
            }

            var pairs = await _assetsService.AssetPairGetAllAsync();
            foreach (var assetPair in pairs)
            {
                if ((assetPair.BaseAssetId != asset1 || assetPair.QuotingAssetId != asset2)
                    && (assetPair.BaseAssetId != asset2 || assetPair.QuotingAssetId != asset1))
                    continue;

                string pairId = $"{asset1}{asset2}";
                _pairsDict.TryAdd(pairId, assetPair);
            }

            throw new UnknownPairException($"Unknown pair of assets: {asset1} and {asset2}!");
        }

        private async Task<string> SearchForPairIdAsync(string asset1, string asset2)
        {
            if (asset1.Length > 20 || asset2.Length > 20) //any assetId is guid
                return null;

            string id1 = $"{asset1}{asset2}";
            if (_pairsDict.ContainsKey(id1))
                return id1;

            string id2 = $"{asset2}{asset1}";
            if (_pairsDict.ContainsKey(id2))
                return id2;

            var pair = await _assetsService.AssetPairGetAsync(id1);
            if (pair != null)
            {
                _pairsDict.TryAdd(pair.Id, pair);
                return pair.Id;
            }

            pair = await _assetsService.AssetPairGetAsync(id2);
            if (pair != null)
            {
                _pairsDict.TryAdd(pair.Id, pair);
                return pair.Id;
            }

            return null;
        }

        private string CleanupNameForTable(string name)
        {
            return name.Replace(" ", "").Replace("_", "").Replace("-", "").ToUpper();
        }
    }
}
