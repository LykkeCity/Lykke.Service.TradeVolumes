using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.TradesConverter.Contract;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Core.Repositories;

namespace Lykke.Service.TradeVolumes.Services
{
    public class TradeVolumesCalculator : TimerPeriod, ITradeVolumesCalculator
    {
        private readonly IAssetsDictionary _assetsDictionary;
        private readonly ITradeVolumesRepository _tradeVolumesRepository;
        private readonly ILog _log;
        private readonly ICachesManager _cachesManager;
        private readonly ConcurrentDictionary<string, (List<string>, DateTime)> _tradesDict =
            new ConcurrentDictionary<string, (List<string>, DateTime)>();
        private readonly TimeSpan _warningDelay;
        private readonly TimeSpan _cacheTimeout;

        private DateTime? _lastProcessedDate;
        private DateTime _lastWarningTime;

        public TradeVolumesCalculator(
            IAssetsDictionary assetsDictionary,
            ICachesManager cachesManager,
            ITradeVolumesRepository tradeVolumesRepository,
            TimeSpan warningDelay,
            TimeSpan cacheTimeout,
            ILog log)
            : base((int)cacheTimeout.TotalMilliseconds, log)
        {
            _assetsDictionary = assetsDictionary;
            _tradeVolumesRepository = tradeVolumesRepository;
            _log = log;
            _cachesManager = cachesManager;
            _warningDelay = warningDelay;
            _cacheTimeout = cacheTimeout;
        }

        public async Task AddTradeLogItemsAsync(List<TradeLogItem> items)
        {
            var walletsMap = new Dictionary<string, string>(items.Count);
            foreach (var item in items)
            {
                walletsMap[item.WalletId] = item.UserId;
            }

            var byHour = items.GroupBy(i => i.DateTime.Hour);
            foreach (var hourGroup in byHour)
            {
                var byAsset = hourGroup.GroupBy(i => i.Asset);
                foreach (var assetGroup in byAsset)
                {
                    var byOppositeAsset = assetGroup.GroupBy(i => i.OppositeAsset);
                    foreach (var oppositeAssetGroup in byOppositeAsset)
                    {
                        var groupTime = oppositeAssetGroup.Max(i => i.DateTime);
                        var walletsVolumes = await _tradeVolumesRepository.GetUserWalletsTradeVolumesAsync(
                            groupTime,
                            walletsMap.Select(i => (i.Value, i.Key)),
                            assetGroup.Key,
                            oppositeAssetGroup.Key);

                        var usersDict = new Dictionary<string, (string, double[])>();
                        var walletsDict = new Dictionary<string, (string, double[])>();
                        foreach (var walletVolumes in walletsVolumes)
                        {
                            string userId = walletsMap[walletVolumes.Key];
                            if (usersDict.ContainsKey(userId))
                            {
                                var userVolumes = usersDict[userId].Item2;
                                userVolumes[0] += walletVolumes.Value[0];
                                userVolumes[1] += walletVolumes.Value[1];
                            }
                            else
                            {
                                usersDict.Add(userId, (walletVolumes.Key, new double[2] { walletVolumes.Value[0], walletVolumes.Value[1] }));
                            }
                            walletsDict[walletVolumes.Key] = (walletsMap[walletVolumes.Key], new double[2] { walletVolumes.Value[2], walletVolumes.Value[3] });
                            _cachesManager.ClearClientCache($"{userId}_{true}");
                            _cachesManager.ClearClientCache($"{walletVolumes.Key}_{false}");
                        }
                        foreach (var item in oppositeAssetGroup)
                        {
                            if (!_tradesDict.ContainsKey(item.TradeId)
                                || !_tradesDict[item.TradeId].Item1.Contains(item.Asset))
                            {
                                var userVolumes = usersDict[item.UserId].Item2;
                                userVolumes[0] += (double)item.Volume;
                                userVolumes[1] += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
                                if (!_tradesDict.ContainsKey(item.TradeId))
                                    _tradesDict.TryAdd(item.TradeId, (new List<string>(2) { item.Asset }, DateTime.UtcNow));
                                else
                                    _tradesDict[item.TradeId].Item1.Add(item.Asset);
                            }
                            else
                            {
                                var tradeAssets = _tradesDict[item.TradeId].Item1;
                                if (tradeAssets.Count == 1)
                                    _tradesDict.TryRemove(item.TradeId, out _);
                                else
                                    tradeAssets.Remove(item.Asset);
                            }

                            var walletVolumes = walletsDict[item.WalletId].Item2;
                            walletVolumes[0] += (double)item.Volume;
                            walletVolumes[1] += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
                        }

                        await _tradeVolumesRepository.NotThreadSafeTradeVolumesUpdateAsync(
                            groupTime,
                            assetGroup.Key,
                            oppositeAssetGroup.Key,
                            usersDict.Select(p => (p.Key, p.Value.Item1, p.Value.Item2[0], p.Value.Item2[1])).ToList(),
                            walletsDict.Select(p => (p.Value.Item1, p.Key, p.Value.Item2[0], p.Value.Item2[1])).ToList());
                    }
                }
            }

            var dateTime = items.Max(i => i.DateTime);
            if (_lastProcessedDate.HasValue)
            {
                var missingDelay = dateTime.Subtract(_lastProcessedDate.Value);
                var now = DateTime.UtcNow;
                if (missingDelay >= _warningDelay && now.Subtract(_lastWarningTime).TotalMinutes >= 1)
                {
                    await _log.WriteWarningAsync(
                        nameof(TradeVolumesCalculator),
                        nameof(AddTradeLogItemsAsync),
                        $"Tradelog items are missing for {missingDelay.TotalMinutes} minutes");
                    _lastWarningTime = now;
                }
            }
            if (!_lastProcessedDate.HasValue || dateTime > _lastProcessedDate.Value)
                _lastProcessedDate = dateTime;
        }

        public override Task Execute()
        {
            var now = DateTime.UtcNow;
            var keysToDelete = new List<string>();
            foreach (var tradeInfo in _tradesDict)
            {
                if (tradeInfo.Value.Item2.Subtract(now) >= _cacheTimeout)
                    keysToDelete.Add(tradeInfo.Key);
            }
            foreach (var key in keysToDelete)
            {
                _tradesDict.TryRemove(key, out _);
            }

            return Task.CompletedTask;
        }

        public async Task<(double, double)> GetPeriodAssetPairVolumeAsync(
            string assetPairId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser)
        {
            var lastProcessedDate = _lastProcessedDate.HasValue
                ? _lastProcessedDate.Value.RoundToHour()
                : DateTime.UtcNow.RoundToHour();
            if (lastProcessedDate < to)
                to = lastProcessedDate;

            if (_cachesManager.TryGetAssetPairTradeVolume(
                $"{clientId}_{isUser}",
                assetPairId,
                from,
                to,
                out (double, double) cachedResult))
                return cachedResult;

            (string baseAssetId, string quotingAssetId) = await _assetsDictionary.GetAssetIdsAsync(assetPairId);
            (double baseVolume, double quotingVolume) = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                baseAssetId,
                quotingAssetId,
                clientId,
                from,
                to,
                isUser);
            baseVolume = Math.Round(baseVolume, 8);
            quotingVolume = Math.Round(quotingVolume, 8);
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
            var lastProcessedDate = _lastProcessedDate.HasValue
                ? _lastProcessedDate.Value.RoundToHour()
                : DateTime.UtcNow.RoundToHour();
            if (lastProcessedDate < to)
                to = lastProcessedDate;

            if (_cachesManager.TryGetAssetTradeVolume(
                $"{clientId}_{isUser}",
                assetId,
                from,
                to,
                out double cachedResult))
                return cachedResult;

            (double result, _) = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                assetId,
                null,
                clientId,
                from,
                to,
                isUser);

            result = Math.Round(result, 8);

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
