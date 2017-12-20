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

        private DateTime _lastProcessedDate;
        private DateTime _lastWarningTime;
        private TimeSpan _warningDelay;

        public TradeVolumesCalculator(
            IAssetsDictionary assetsDictionary,
            ICachesManager cachesManager,
            ITradeVolumesRepository tradeVolumesRepository,
            TimeSpan warningDelay,
            ILog log)
        {
            _assetsDictionary = assetsDictionary;
            _tradeVolumesRepository = tradeVolumesRepository;
            _log = log;
            _cachesManager = cachesManager;
            _lastProcessedDate = DateTime.UtcNow.Date;
            _warningDelay = warningDelay;
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

            if (item.DateTime > _lastProcessedDate)
                _lastProcessedDate = item.DateTime;

            DateTime now = DateTime.UtcNow;
            if (now.Subtract(_lastProcessedDate) >= _warningDelay && now.Subtract(_lastWarningTime).TotalMinutes >= 1)
            {
                await _log.WriteWarningAsync(
                    nameof(TradeVolumesCalculator),
                    nameof(AddTradeLogItemAsync),
                    $"Tradelog items are not ");
                _lastWarningTime = now;
            }
        }

        public async Task<(double, double)> GetPeriodAssetPairVolumeAsync(
            string assetPairId,
            string clientId,
            DateTime from,
            DateTime to)
        {
            clientId = ClientIdHashHelper.GetClientIdHash(clientId);
            var lastProcessedDate = _lastProcessedDate.RoundToHour();
            if (lastProcessedDate < to)
                to = lastProcessedDate;

            if (_cachesManager.TryGetAssetPairTradeVolume(
                clientId,
                assetPairId,
                from,
                to,
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
            var lastProcessedDate = _lastProcessedDate.RoundToHour();
            if (lastProcessedDate < to)
                to = lastProcessedDate;

            if (_cachesManager.TryGetAssetTradeVolume(
                clientId,
                assetId,
                from,
                to,
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
                result);

            return result;
        }
    }
}
