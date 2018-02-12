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
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, (List<string>, DateTime)>> _tradesDict =
            new ConcurrentDictionary<string, ConcurrentDictionary<string, (List<string>, DateTime)>>();
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
                        var usersHash = new HashSet<string>(items.Count);
                        var walletsHash = new HashSet<string>(items.Count);
                        foreach (var item in oppositeAssetGroup)
                        {
                            usersHash.Add(item.UserId);
                            walletsHash.Add(item.WalletId);
                        }

                        var volumes = await _tradeVolumesRepository.GetUserWalletsTradeVolumesAsync(
                            groupTime,
                            usersHash,
                            walletsHash,
                            assetGroup.Key,
                            oppositeAssetGroup.Key);

                        foreach (var item in oppositeAssetGroup)
                        {
                            if (!_tradesDict.ContainsKey(item.TradeId)
                                || !_tradesDict[item.TradeId].ContainsKey(item.UserId)
                                || !_tradesDict[item.TradeId][item.UserId].Item1.Contains(item.Asset))
                            {
                                var userVolumes = volumes[item.UserId];
                                userVolumes[0] += (double)item.Volume;
                                userVolumes[1] += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
                                if (!_tradesDict.ContainsKey(item.TradeId))
                                {
                                    var usersDataDict = new ConcurrentDictionary<string, (List<string>, DateTime)>();
                                    usersDataDict.TryAdd(item.UserId, (new List<string>(2) { item.Asset }, DateTime.UtcNow));
                                    _tradesDict.TryAdd(item.TradeId, usersDataDict);
                                }
                                else
                                {
                                    var usersDataDict = _tradesDict[item.TradeId];
                                    if (!usersDataDict.ContainsKey(item.UserId))
                                        usersDataDict.TryAdd(item.UserId, (new List<string>(2) { item.Asset }, DateTime.UtcNow));
                                    else
                                        usersDataDict[item.UserId].Item1.Add(item.Asset);
                                }
                            }
                            else
                            {
                                var tradeUsersData = _tradesDict[item.TradeId];
                                var tradeAssets = tradeUsersData[item.UserId].Item1;
                                if (tradeAssets.Count == 1)
                                {
                                    if (tradeUsersData.Count == 1)
                                        _tradesDict.TryRemove(item.TradeId, out _);
                                    else
                                        tradeUsersData.TryRemove(item.UserId, out _);
                                }
                                else
                                {
                                    tradeAssets.Remove(item.Asset);
                                }
                            }

                            var walletVolumes = volumes[item.WalletId];
                            walletVolumes[0] += (double)item.Volume;
                            walletVolumes[1] += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
                        }

                        await _tradeVolumesRepository.NotThreadSafeTradeVolumesUpdateAsync(
                            groupTime,
                            assetGroup.Key,
                            oppositeAssetGroup.Key,
                            usersHash.Select(u => (u, volumes[u][0], volumes[u][1])).ToList(),
                            walletsHash.Select(w => (walletsMap[w], w, volumes[w][0], volumes[w][1])).ToList());

                        foreach (var userId in usersHash)
                        {
                            _cachesManager.ClearClientCache(userId);
                        }
                        foreach (var walletId in walletsHash)
                        {
                            _cachesManager.ClearClientCache(walletId);
                        }
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
                foreach (var tradeUserInfo in tradeInfo.Value)
                {
                    if (tradeUserInfo.Value.Item2.Subtract(now) >= _cacheTimeout)
                        keysToDelete.Add(tradeInfo.Key);
                }
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
                clientId,
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
            DateTime to,
            bool isUser)
        {
            var lastProcessedDate = _lastProcessedDate.HasValue
                ? _lastProcessedDate.Value.RoundToHour()
                : DateTime.UtcNow.RoundToHour();
            if (lastProcessedDate < to)
                to = lastProcessedDate;

            if (_cachesManager.TryGetAssetTradeVolume(
                clientId,
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
                clientId,
                assetId,
                from,
                to,
                result);

            return result;
        }
    }
}
