using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Service.Assets.Client;
using Lykke.Service.Assets.Client.Models;
using Lykke.Service.TradeVolumes.Core;
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
            (double tradeVolume, double oppositeTradeVolume) = await _tradeVolumesRepository.GetClientPairValuesAsync(
                item.DateTime,
                item.UserId,
                item.Asset,
                item.OppositeAsset,
                Constants.AllClients);

            tradeVolume += (double)item.Volume;
            await _tradeVolumesRepository.UpdateTradeVolumesForBothAssetsAsync(
                item.DateTime,
                item.UserId,
                item.Asset,
                tradeVolume,
                item.OppositeAsset,
                oppositeTradeVolume);
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
                to,
                clientId == Constants.AllClients ? null : Constants.AllClients);
            double quotingVolume = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                quotingAssetId,
                baseAssetId,
                clientId,
                from,
                to,
                clientId == Constants.AllClients ? null : Constants.AllClients);
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
                to,
                clientId == Constants.AllClients ? null : Constants.AllClients);
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
                throw new UnknownPairException($"Unknown assetPair {assetPair}!");

            return (_pairsDict[assetPair].BaseAssetId, _pairsDict[assetPair].QuotingAssetId);
        }
    }
}
