using System.Linq;
using System.Collections.Generic;
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
        private readonly Dictionary<string, AssetPair> _pairsDict = new Dictionary<string, AssetPair>();
        private readonly Dictionary<string, List<string>> _possibleTableNamesDict = new Dictionary<string, List<string>>();

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

                    string possibleTableName = string.Format(
                        Constants.TableNameFormat,
                        asset,
                        asset2);
                    possibleTableNames.Add(possibleTableName);
                }
                _possibleTableNamesDict.Add(asset, possibleTableNames);
            }

            if (!_possibleTableNamesDict.ContainsKey(assetId))
                throw new UnknownAssetException($"Unknown asset {assetId}");

            return _possibleTableNamesDict[assetId];
        }

        public async Task<(string, string)> GetAssetIdsAsync(string assetPair)
        {
            if (_pairsDict.ContainsKey(assetPair))
                return (_pairsDict[assetPair].BaseAssetId, _pairsDict[assetPair].QuotingAssetId);

            var pairs = await _assetsService.AssetPairGetAllAsync();
            foreach (var pair in pairs)
            {
                if (_pairsDict.ContainsKey(pair.Id))
                    continue;

                _pairsDict.Add(pair.Id, pair);
            }

            if (!_pairsDict.ContainsKey(assetPair))
                throw new UnknownPairException($"Unknown assetPair {assetPair}!");

            return (_pairsDict[assetPair].BaseAssetId, _pairsDict[assetPair].QuotingAssetId);
        }
    }
}
