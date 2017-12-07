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
        private readonly Dictionary<string, Dictionary<string, double>> _allDict = new Dictionary<string, Dictionary<string, double>>();

        private DateTime _allClientsDate = DateTime.MinValue;

        public const string AllClients = "all";

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
                item.OppositeAsset,
                AllClients);

            await _tradeVolumesRepository.UpdateTradeVolumesAsync(
                item.DateTime,
                item.UserId,
                item.Asset,
                baseTradeVolume + (double)item.Volume,
                item.OppositeAsset,
                quotingTradeVolume + (item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0));

            if (_allClientsDate.Date != item.DateTime.Date)
            {
                _allDict.Clear();
                _allClientsDate = item.DateTime.Date;
            }
            if (!_allDict.ContainsKey(item.Asset) || !_allDict.ContainsKey(item.OppositeAsset))
            {
                (double baseAllTradeVolume, double quotingAllTradeVolume) = await _tradeVolumesRepository.GetClientPairValuesAsync(
                    item.DateTime,
                    AllClients,
                    item.Asset,
                    item.OppositeAsset,
                    null);

                AddAllVolume(item.Asset, item.OppositeAsset, baseAllTradeVolume, true);
                AddAllVolume(item.OppositeAsset, item.Asset, quotingAllTradeVolume, true);
            }
            double allBase = AddAllVolume(item.Asset, item.OppositeAsset, baseTradeVolume, false);
            double allQuoting = AddAllVolume(item.OppositeAsset, item.Asset, quotingTradeVolume, false);
            await _tradeVolumesRepository.UpdateTradeVolumesAsync(
                item.DateTime,
                AllClients,
                item.Asset,
                allBase,
                item.OppositeAsset,
                allQuoting);
        }

        private double AddAllVolume(string firstAsset, string secondAsset, double volume, bool replace)
        {
            if (!_allDict.ContainsKey(firstAsset))
            {
                _allDict.Add(firstAsset, new Dictionary<string, double> { { secondAsset, volume } });
                return volume;
            }
            var dict = _allDict[firstAsset];
            if (!dict.ContainsKey(secondAsset) || replace)
            {
                dict[secondAsset] = volume;
                return volume;
            }
            dict[secondAsset] += volume;
            return dict[secondAsset];
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
                clientId == AllClients ? null : AllClients);
            double quotingVolume = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                quotingAssetId,
                baseAssetId,
                clientId,
                from,
                to,
                clientId == AllClients ? null : AllClients);
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
                clientId == AllClients ? null : AllClients);
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
