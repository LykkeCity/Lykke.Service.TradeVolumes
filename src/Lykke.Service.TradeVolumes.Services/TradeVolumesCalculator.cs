﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.TradesConverter.Contract;
using Lykke.Service.TradeVolumes.Core;
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
        private readonly int _cacheWarningCount;
        private readonly TimeSpan _cacheTimeout;

        private DateTime? _lastProcessedDate;
        private DateTime _lastWarningTime = DateTime.MinValue;

        public TradeVolumesCalculator(
            IAssetsDictionary assetsDictionary,
            ICachesManager cachesManager,
            ITradeVolumesRepository tradeVolumesRepository,
            TimeSpan warningDelay,
            TimeSpan cacheTimeout,
            int cacheWarningCount,
            ILog log)
            : base((int)cacheTimeout.TotalMilliseconds, log)
        {
            _assetsDictionary = assetsDictionary;
            _tradeVolumesRepository = tradeVolumesRepository;
            _log = log;
            _cachesManager = cachesManager;
            _warningDelay = warningDelay;
            _cacheWarningCount = cacheWarningCount;
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

                        var volumesDict = await _tradeVolumesRepository.GetUserWalletsTradeVolumesAsync(
                            groupTime,
                            usersHash,
                            walletsHash,
                            assetGroup.Key,
                            oppositeAssetGroup.Key);

                        foreach (var item in oppositeAssetGroup)
                        {
                            await ProcessItemAsync(
                                item,
                                assetGroup.Key,
                                oppositeAssetGroup.Key,
                                volumesDict,
                                items);
                        }

                        await _tradeVolumesRepository.NotThreadSafeTradeVolumesUpdateAsync(
                            groupTime,
                            assetGroup.Key,
                            oppositeAssetGroup.Key,
                            usersHash
                                .Select(u =>
                                    {
                                        var userVolume = volumesDict[_tradeVolumesRepository.GetUserVolumeKey(u)];
                                        return (u, userVolume[0], userVolume[1]);
                                    })
                                .ToList(),
                            walletsHash
                                .Select(w =>
                                    {
                                        var walletVolume = volumesDict[_tradeVolumesRepository.GetWalletVolumeKey(w)];
                                        return (walletsMap[w], w, walletVolume[0], walletVolume[1]);
                                    })
                                .ToList());
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
                    _log.WriteWarning(
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

            var tradesToRemove = new List<string>();
            var tradeIds = new List<string>(_tradesDict.Keys);
            foreach (var tradeId in tradeIds)
            {
                if (!_tradesDict.TryGetValue(tradeId, out var tradeInfo))
                    continue;

                var usersToRemove = new List<string>();
                var tradeUsers = new List<string>(tradeInfo.Keys);
                foreach (var tradeUser in tradeUsers)
                {
                    if (!tradeInfo.TryGetValue(tradeUser, out var userTradesInfo))
                        continue;

                    if (now.Subtract(userTradesInfo.Item2) >= _cacheTimeout)
                        usersToRemove.Add(tradeUser);
                }
                foreach (var user in usersToRemove)
                {
                    tradeInfo.TryRemove(user, out _);
                }
                if (tradeInfo.Count == 0)
                    tradesToRemove.Add(tradeId);
            }
            foreach (var tradeId in tradesToRemove)
            {
                _tradesDict.TryRemove(tradeId, out _);
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
                to = lastProcessedDate.AddHours(1);

            if (clientId != Constants.AllClients
                && to.Subtract(from).TotalDays <= Constants.MaxPeriodInDays
                && _cachesManager.TryGetAssetPairTradeVolume(
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
            var result = (baseVolume, quotingVolume);

            if (clientId != Constants.AllClients
                && to.Subtract(from).TotalDays <= Constants.MaxPeriodInDays)
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
                to = lastProcessedDate.AddHours(1);

            if (clientId != Constants.AllClients
                && to.Subtract(from).TotalDays <= Constants.MaxPeriodInDays
                && _cachesManager.TryGetAssetTradeVolume(
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

            if (clientId != Constants.AllClients
                && to.Subtract(from).TotalDays <= Constants.MaxPeriodInDays)
                _cachesManager.AddAssetTradeVolume(
                    clientId,
                    assetId,
                    from,
                    to,
                    result);

            return result;
        }

        private async Task UpdateVolumesCacheAsync(
            string assetId,
            string oppositeAssetId,
            string clientId,
            DateTime time,
            double baseVolume,
            double quotingVolume)
        {
            _cachesManager.UpdateAssetTradeVolume(
                clientId,
                assetId,
                time,
                baseVolume);

            var assetPairId = await _assetsDictionary.GetAssetPairIdAsync(assetId, oppositeAssetId);
            (string baseAssetId, string quotingAssetId) = await _assetsDictionary.GetAssetIdsAsync(assetPairId);
            if (assetId != baseAssetId)
                return;

            _cachesManager.UpdateAssetPairTradeVolume(
                    clientId,
                    assetPairId,
                    time,
                    (baseVolume, quotingVolume));
        }

        private async Task ProcessItemAsync(
            TradeLogItem item,
            string assetId,
            string oppositeAssetId,
            Dictionary<string, double[]> volumesDict,
            List<TradeLogItem> items)
        {
            var userVolumes = volumesDict[_tradeVolumesRepository.GetUserVolumeKey(item.UserId)];
            bool doNotUseTradesCache = items.Any(i =>
                i != item && i.TradeId == item.TradeId && i.Asset == item.Asset && i.UserId != item.UserId);

            if (doNotUseTradesCache)
            {
                userVolumes[0] += (double)item.Volume;
                userVolumes[1] += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
                await UpdateVolumesCacheAsync(
                    assetId,
                    oppositeAssetId,
                    item.UserId,
                    item.DateTime,
                    (double)item.Volume,
                    item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0);
            }
            else
            {
                await UpdateTradesCacheAsync(
                    item,
                    assetId,
                    oppositeAssetId,
                    userVolumes);
            }

            var walletVolumes = volumesDict[_tradeVolumesRepository.GetWalletVolumeKey(item.WalletId)];
            walletVolumes[0] += (double)item.Volume;
            walletVolumes[1] += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;

            await UpdateVolumesCacheAsync(
                assetId,
                oppositeAssetId,
                item.WalletId,
                item.DateTime,
                (double)item.Volume,
                item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0);
        }

        private async Task UpdateTradesCacheAsync(
            TradeLogItem item,
            string assetId,
            string oppositeAssetId,
            double[] userVolumes)
        {
            if (!_tradesDict.ContainsKey(item.TradeId)
                || !_tradesDict[item.TradeId].ContainsKey(item.UserId)
                || !_tradesDict[item.TradeId][item.UserId].Item1.Contains(item.Asset))
            {
                userVolumes[0] += (double)item.Volume;
                userVolumes[1] += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
                await UpdateVolumesCacheAsync(
                    assetId,
                    oppositeAssetId,
                    item.UserId,
                    item.DateTime,
                    (double)item.Volume,
                    item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0);
                if (_tradesDict.ContainsKey(item.TradeId))
                {
                    var usersDataDict = _tradesDict[item.TradeId];
                    if (!usersDataDict.ContainsKey(item.UserId))
                        usersDataDict.TryAdd(item.UserId, (new List<string>(2) { item.Asset }, DateTime.UtcNow));
                    else
                        usersDataDict[item.UserId].Item1.Add(item.Asset);
                }
                else
                {
                    var usersDataDict = new ConcurrentDictionary<string, (List<string>, DateTime)>();
                    usersDataDict.TryAdd(item.UserId, (new List<string>(2) { item.Asset }, DateTime.UtcNow));
                    _tradesDict.TryAdd(item.TradeId, usersDataDict);

                    if (_tradesDict.Count > _cacheWarningCount)
                    {
                        var now = DateTime.UtcNow;
                        if (now.Subtract(_lastWarningTime).TotalMinutes >= 1)
                        {
                            _log.WriteWarning(
                                nameof(TradeVolumesCalculator),
                                nameof(AddTradeLogItemsAsync),
                                $"Tradelog items cache has {_tradesDict.Count} items!");
                            _lastWarningTime = now;
                        }
                    }
                }
            }
            else
            {
                userVolumes[0] -= (double)item.Volume;
                userVolumes[1] -= item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
                await UpdateVolumesCacheAsync(
                    assetId,
                    oppositeAssetId,
                    item.UserId,
                    item.DateTime,
                    (double)-item.Volume,
                    item.OppositeVolume.HasValue ? (double)-item.OppositeVolume.Value : 0);

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
        }
    }
}
