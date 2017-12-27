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

        private DateTime? _lastProcessedDate;
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
            _warningDelay = warningDelay;
        }

        public async Task AddTradeLogItemAsync(TradeLogItem item)
        {
            (var baseUserVolume, var quotingUserVolume, var baseWalletVolume, var quotingWalletVolume) =
                await _tradeVolumesRepository.GetClientPairValuesAsync(
                    item.DateTime,
                    item.UserId,
                    item.WalletId,
                    item.Asset,
                    item.OppositeAsset);

            baseUserVolume += (double)item.Volume;
            quotingUserVolume += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
            baseWalletVolume += (double)item.Volume;
            quotingWalletVolume += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;

            await _tradeVolumesRepository.NotThreadSafeTradeVolumesUpdateAsync(
                item.DateTime,
                item.UserId,
                item.WalletId,
                item.Asset,
                item.OppositeAsset,
                baseUserVolume,
                quotingUserVolume,
                baseWalletVolume,
                quotingWalletVolume);

            if (!_lastProcessedDate.HasValue || item.DateTime > _lastProcessedDate)
                _lastProcessedDate = item.DateTime;

            DateTime now = DateTime.UtcNow;
            if (now.Subtract(_lastProcessedDate.Value) >= _warningDelay && now.Subtract(_lastWarningTime).TotalMinutes >= 1)
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
            DateTime to,
            bool isUser)
        {
            if (_lastProcessedDate.HasValue)
            {
                var lastProcessedDate = _lastProcessedDate.Value.RoundToHour();
                if (lastProcessedDate < to)
                    to = lastProcessedDate;
            }

            if (_cachesManager.TryGetAssetPairTradeVolume(
                $"{clientId}_{isUser}",
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
                to,
                isUser);
            double quotingVolume = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                quotingAssetId,
                baseAssetId,
                clientId,
                from,
                to,
                isUser);
            var result = (baseVolume, quotingVolume);

            _cachesManager.AddAssetPairTradeVolume(
                $"{clientId}_{isUser}",
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
            DateTime to,
            bool isUser)
        {
            if (_lastProcessedDate.HasValue)
            {
                var lastProcessedDate = _lastProcessedDate.Value.RoundToHour();
                if (lastProcessedDate < to)
                    to = lastProcessedDate;
            }

            if (_cachesManager.TryGetAssetTradeVolume(
                $"{clientId}_{isUser}",
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
                to,
                isUser);

            _cachesManager.AddAssetTradeVolume(
                $"{clientId}_{isUser}",
                assetId,
                from,
                to,
                result);

            return result;
        }
    }
}
