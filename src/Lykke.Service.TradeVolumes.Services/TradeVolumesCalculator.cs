﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.TradesConverter.Contract;
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

                        var usersDict = new Dictionary<string, (string, double[], HashSet<string>)>();
                        var walletsDict = new Dictionary<string, (string, double[])>();
                        foreach (var walletVolumes in walletsVolumes)
                        {
                            usersDict[walletsMap[walletVolumes.Key]] =
                                (walletVolumes.Key, new double[2] { walletVolumes.Value[0], walletVolumes.Value[1] }, new HashSet<string>());
                            walletsDict[walletVolumes.Key] =
                                (walletsMap[walletVolumes.Key], new double[2] { walletVolumes.Value[2], walletVolumes.Value[3] });
                        }
                        foreach (var item in oppositeAssetGroup)
                        {
                            var userData = usersDict[item.UserId];
                            if (!userData.Item3.Contains(item.TradeId))
                            {
                                userData.Item2[0] += (double)item.Volume;
                                userData.Item2[1] += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
                                userData.Item3.Add(item.TradeId);
                            }

                            var walletData = walletsDict[item.WalletId];
                            walletData.Item2[0] += (double)item.Volume;
                            walletData.Item2[1] += item.OppositeVolume.HasValue ? (double)item.OppositeVolume.Value : 0;
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
            double baseVolume = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                baseAssetId,
                quotingAssetId,
                clientId,
                from,
                to,
                isUser);
            baseVolume = Math.Round(baseVolume, 8);
            double quotingVolume = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                quotingAssetId,
                baseAssetId,
                clientId,
                from,
                to,
                isUser);
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

            double result = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
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
