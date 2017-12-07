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
                item.OppositeAsset);

            await _tradeVolumesRepository.UpdateTradeVolumesAsync(
                item.DateTime,
                item.UserId,
                item.Asset,
                baseTradeVolume + (double)item.Volume,
                item.OppositeAsset,
                quotingTradeVolume + (item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0));

            if (_allClientsDate.Date != item.DateTime.Date)
                _allDict.Clear();
            if (!_allDict.ContainsKey(item.Asset) || !_allDict.ContainsKey(item.OppositeAsset))
            {
                (double baseAllTradeVolume, double quotingAllTradeVolume) = await _tradeVolumesRepository.GetClientPairValuesAsync(
                    item.DateTime,
                    AllClients,
                    item.Asset,
                    item.OppositeAsset);

                AddVolume(item.Asset, item.OppositeAsset, baseAllTradeVolume);
                AddVolume(item.OppositeAsset, item.Asset, quotingAllTradeVolume);
            }
            AddVolume(item.Asset, item.OppositeAsset, baseTradeVolume);
            AddVolume(item.OppositeAsset, item.Asset, quotingTradeVolume);
        }

        private void AddVolume(string firstAsset, string secondAsset, double volume)
        {
            if (!_allDict.ContainsKey(firstAsset))
            {
                _allDict.Add(firstAsset, new Dictionary<string, double> { { secondAsset, volume } });
            }
            else
            {
                var dict = _allDict[firstAsset];
                if (!dict.ContainsKey(secondAsset))
                    dict.Add(secondAsset, volume);
                else
                    dict[secondAsset] += volume;
            }
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
