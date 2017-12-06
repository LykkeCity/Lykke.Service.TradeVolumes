using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Service.Assets.Client;
using Lykke.Service.Assets.Client.Models;
using Lykke.Service.TradeVolumes.Core.Messages;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Core.Repositories;

namespace Lykke.Service.TradeVolumes.Services
{
    public class TradeVolumesCalculator : ITradeVolumesCalculator
    {
        private readonly IAssetsService _assetsService;
        private readonly ITradeVolumesRepository _tradeVolumesRepository;
        private readonly Dictionary<string, AssetPair> _pairsDict;

        public TradeVolumesCalculator(
            IAssetsService assetsService,
            ITradeVolumesRepository tradeVolumesRepository)
        {
            _assetsService = assetsService;
            _tradeVolumesRepository = tradeVolumesRepository;

            var pairs = _assetsService.AssetPairGetAllAsync().Result;
            _pairsDict = pairs.ToDictionary(i => i.Id, i => i);
        }

        public async Task AddTradeLogItemAsync(TradeLogItem item)
        {
            (double baseTradeVolume, double quotingTradeVolume) = await _tradeVolumesRepository.GetClientPairValuesAsync(
                item.DateTime,
                item.UserId,
                item.Asset,
                item.OppositeAsset);

            await _tradeVolumesRepository.UpdateTradeVolumesAsync(
                item.DateTime,
                item.UserId,
                item.Asset,
                baseTradeVolume + (double)item.Volume,
                item.OppositeAsset,
                quotingTradeVolume + (item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0));
        }

        public async Task<(double, double)> GetPeriodAssetPairVolumeAsync(
            string assetPairId,
            string clientId,
            DateTime from,
            DateTime to)
        {
            clientId = ClientIdHashHelper.GetClientIdHash(clientId);
            (string baseAssetId, string quotingAssetId) = GetAssetIds(assetPairId);
            double baseVolume = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                baseAssetId,
                quotingAssetId,
                clientId,
                from,
                to);
            double quotingVolume = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                quotingAssetId,
                baseAssetId,
                clientId,
                from,
                to);
            return (baseVolume, quotingVolume);
        }

        public async Task<double> GetPeriodAssetVolumeAsync(
            string assetId,
            string clientId,
            DateTime from,
            DateTime to)
        {
            clientId = ClientIdHashHelper.GetClientIdHash(clientId);
            return await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                assetId,
                null,
                clientId,
                from,
                to);
        }

        private (string,string) GetAssetIds(string assetPair)
        {
            if (_pairsDict.ContainsKey(assetPair))
                return (_pairsDict[assetPair].BaseAssetId, _pairsDict[assetPair].QuotingAssetId);

            var pairs = _assetsService.AssetPairGetAllAsync().Result;
            foreach (var pair in pairs)
            {
                if (_pairsDict.ContainsKey(pair.Id))
                    continue;

                _pairsDict.Add(pair.Id, pair);
            }

            if (!_pairsDict.ContainsKey(assetPair))
                throw new ArgumentOutOfRangeException($"Unknown assetPair {assetPair}!");

            return (_pairsDict[assetPair].BaseAssetId, _pairsDict[assetPair].QuotingAssetId);
        }
    }
}
