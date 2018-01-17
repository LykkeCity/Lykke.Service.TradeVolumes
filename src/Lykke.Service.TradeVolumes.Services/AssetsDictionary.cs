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

        public AssetsDictionary(IAssetsService assetsService)
        {
            _assetsService = assetsService;
        }

        public async Task<List<string>> GeneratePossibleTableNamesAsync(string assetId)
        {
            assetId = assetId.Replace("-", "");
            if (_possibleTableNamesDict.ContainsKey(assetId))
                return _possibleTableNamesDict[assetId];

            var assets = await _assetsService.AssetGetAllAsync(true);
            var assetIds = assets.Select(a => a.Id.Replace("-", "")).ToList();
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
                        asset.ToUpper(),
                        asset2.ToUpper());
                    possibleTableNames.Add(possibleTableName);
                }
                _possibleTableNamesDict.TryAdd(asset, possibleTableNames);
            }

            if (!_possibleTableNamesDict.ContainsKey(assetId))
                throw new UnknownAssetException($"Unknown asset {assetId}");

            return _possibleTableNamesDict[assetId];
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
    }
}
