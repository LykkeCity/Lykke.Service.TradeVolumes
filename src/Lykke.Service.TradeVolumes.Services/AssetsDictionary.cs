using System.Linq;
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
        private readonly ConcurrentDictionary<string, List<string>> _possibleTableNamesDict = new ConcurrentDictionary<string, List<string>>();
        private readonly ConcurrentDictionary<string, string> _assetsDict = new ConcurrentDictionary<string, string>();

        public AssetsDictionary(IAssetsService assetsService)
        {
            _assetsService = assetsService;
        }

        public async Task<List<string>> GeneratePossibleTableNamesAsync(string assetId)
        {
            var assetName = await GetShortNameAsync(assetId);
            if (_possibleTableNamesDict.ContainsKey(assetName))
                return _possibleTableNamesDict[assetName];

            var assets = await _assetsService.AssetGetAllAsync(true);
            var assetIds = assets.Select(a => GetShortName(a)).ToHashSet();

            if (!assetIds.Contains(assetName))
                throw new UnknownAssetException($"Unknown asset {assetId}");

            foreach (var asset in assetIds)
            {
                if (_possibleTableNamesDict.ContainsKey(asset))
                    continue;

                var possibleTableNames = new List<string>(assetIds.Count - 1);
                foreach (var asset2 in assetIds)
                {
                    if (asset == asset2)
                        continue;

                    var possibleTableName = string.Format(
                        Constants.TableNameFormat,
                        asset,
                        asset2);
                    possibleTableNames.Add(possibleTableName);
                }
                _possibleTableNamesDict.TryAdd(asset, possibleTableNames);
            }

            return _possibleTableNamesDict[assetName];
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
            if (alias.Length <= 31)
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


        private string GetShortName(Asset asset)
        {
            var assetName = CleanupNameForTable(asset.Id);
            if (assetName.Length <= 31)
                return assetName;

            assetName = string.IsNullOrWhiteSpace(asset.DisplayId) ? asset.Name : asset.DisplayId;
            return CleanupNameForTable(assetName);
        }

        private string CleanupNameForTable(string name)
        {
            return name.Replace(" ", "").Replace("_", "").Replace("-", "").ToUpper();
        }
    }
}
