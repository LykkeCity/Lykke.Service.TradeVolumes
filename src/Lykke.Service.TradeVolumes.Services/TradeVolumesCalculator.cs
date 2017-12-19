using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Service.TradeVolumes.Core.Messages;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Core.Repositories;

namespace Lykke.Service.TradeVolumes.Services
{
    public class TradeVolumesCalculator : ITradeVolumesCalculator
    {
        private readonly IAssetsDictionary _assetsDictionary;
        private readonly ITradeVolumesRepository _tradeVolumesRepository;
        private readonly ILog _log;
        private readonly ICachesManager _cachesManager;

        public TradeVolumesCalculator(
            IAssetsDictionary assetsDictionary,
            ICachesManager cachesManager,
            ITradeVolumesRepository tradeVolumesRepository,
            ILog log)
        {
            _assetsDictionary = assetsDictionary;
            _tradeVolumesRepository = tradeVolumesRepository;
            _log = log;
            _cachesManager = cachesManager;
        }

        public async Task AddTradeLogItemAsync(TradeLogItem item)
        {
            (double tradeVolume, double oppositeTradeVolume) = await _tradeVolumesRepository.GetClientPairValuesAsync(
                item.DateTime,
                item.UserId,
                item.Asset,
                item.OppositeAsset);

            tradeVolume += (double)item.Volume;
            oppositeTradeVolume += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
            await _tradeVolumesRepository.NotThreadSafeTradeVolumesUpdateAsync(
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
            var now = DateTime.UtcNow.RoundToHour();
            if (now < to)
                to = now;

            if (_cachesManager.TryGetAssetPairTradeVolume(
                clientId,
                assetPairId,
                from,
                to,
                now,
                out (double, double) cachedResult))
                return cachedResult;

            (string baseAssetId, string quotingAssetId) = await _assetsDictionary.GetAssetIdsAsync(assetPairId);
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
            var result = (baseVolume, quotingVolume);

            _cachesManager.AddAssetPairTradeVolume(
                clientId,
                assetPairId,
                from,
                to,
                now,
                result);

            return result;
        }

        public async Task<double> GetPeriodAssetVolumeAsync(
            string assetId,
            string clientId,
            DateTime from,
            DateTime to)
        {
            clientId = ClientIdHashHelper.GetClientIdHash(clientId);
            var now = DateTime.UtcNow.RoundToHour();
            if (now < to)
                to = now;

            if (_cachesManager.TryGetAssetTradeVolume(
                clientId,
                assetId,
                from,
                to,
                now,
                out double cachedResult))
                return cachedResult;

            double result = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                assetId,
                null,
                clientId,
                from,
                to);

            _cachesManager.AddAssetTradeVolume(
                clientId,
                assetId,
                from,
                to,
                now,
                result);

            return result;
        }
    }
}
