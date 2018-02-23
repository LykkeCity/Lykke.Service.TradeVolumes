using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Service.TradeVolumes.Core;
using Lykke.Service.TradeVolumes.Core.Services;

namespace Lykke.Service.TradeVolumes.Services
{
    public class CachesManager : TimerPeriod, ICachesManager
    {
        private const int _cacheLifeHoursCount = 24 * Constants.MaxPeriodInDays;

        private readonly ILog _log;
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, double>>>> _assetVolumesCache
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, double>>>>();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, (double, double)>>>> _assetPairVolumesCache
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, (double, double)>>>>();

        private DateTime _lastWarningTime = DateTime.MinValue;

        public CachesManager(ILog log)
            : base((int)TimeSpan.FromMinutes(15).TotalMilliseconds, log)
        {
            _log = log;
        }

        public override Task Execute()
        {
            CleanUpCache(_assetVolumesCache);
            CleanUpCache(_assetPairVolumesCache);

            return Task.CompletedTask;
        }

        public bool TryGetAssetTradeVolume(
            string clientId,
            string assetId,
            DateTime from,
            DateTime to,
            out double result)
        {
            if (IsCahedPeriod(from)
                && _assetVolumesCache.TryGetValue(clientId, out var clientDict)
                && clientDict.TryGetValue(assetId, out var assetDict)
                && assetDict.TryGetValue(from, out var periods)
                && periods.TryGetValue(to, out result))
            {
                return true;
            }
            result = 0;
            return false;
        }

        public void AddAssetTradeVolume(
            string clientId,
            string assetId,
            DateTime from,
            DateTime to,
            double tradeVolume)
        {
            if (!IsCahedPeriod(from))
                return;

            if (!_assetVolumesCache.TryGetValue(clientId, out var clientDict))
            {
                var periodsDict = new ConcurrentDictionary<DateTime, double>();
                periodsDict.TryAdd(to, tradeVolume);
                var assetDict = new ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, double>>();
                assetDict.TryAdd(from, periodsDict);
                clientDict = new ConcurrentDictionary<string, ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, double>>>();
                clientDict.TryAdd(assetId, assetDict);
                _assetVolumesCache.TryAdd(clientId, clientDict);
            }
            else if (!clientDict.TryGetValue(assetId, out var assetDict))
            {
                var periodsDict = new ConcurrentDictionary<DateTime, double>();
                periodsDict.TryAdd(to, tradeVolume);
                assetDict = new ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, double>>();
                assetDict.TryAdd(from, periodsDict);
                clientDict.TryAdd(assetId, assetDict);
            }
            else if (!assetDict.TryGetValue(from, out var periodsDict))
            {
                periodsDict = new ConcurrentDictionary<DateTime, double>();
                periodsDict.TryAdd(to, tradeVolume);
                assetDict.TryAdd(from, periodsDict);
            }
            else
            {
                periodsDict.TryAdd(to, tradeVolume);
            }

            if (_assetVolumesCache.Count > 1000)
            {
                var now = DateTime.UtcNow;
                if (now.Subtract(_lastWarningTime).TotalMinutes >= 1)
                {
                    _log.WriteWarning(nameof(CachesManager), nameof(AddAssetTradeVolume), $"Already {_assetVolumesCache.Count} items in asset cache");
                    _lastWarningTime = now;
                }
            }
        }

        public void UpdateAssetTradeVolume(
            string clientId,
            string assetId,
            DateTime time,
            double tradeVolume)
        {
            if (!_assetVolumesCache.TryGetValue(clientId, out var clientDict))
                return;

            if (!clientDict.TryGetValue(assetId, out var assetDict))
                return;

            var periodStarts = new List<DateTime>(assetDict.Keys);
            foreach (var periodStart in periodStarts)
            {
                if (periodStart > time)
                    continue;

                if (!assetDict.TryGetValue(periodStart, out var periodsDict))
                    continue;

                var periodEnds = new List<DateTime>(periodsDict.Keys);
                foreach (var periodEnd in periodEnds)
                {
                    if (periodEnd < time)
                        continue;

                    if (!periodsDict.TryGetValue(periodEnd, out var volume))
                        continue;

                    periodsDict.TryUpdate(periodEnd, volume, volume + tradeVolume);
                }
            }
        }

        public bool TryGetAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to,
            out (double,double) result)
        {
            if (IsCahedPeriod(from)
                && _assetPairVolumesCache.TryGetValue(clientId, out var clientDict)
                && clientDict.TryGetValue(assetPairId, out var assetPairDict)
                && assetPairDict.TryGetValue(from, out var periodsDict)
                && periodsDict.TryGetValue(to, out result))
            {
                return true;
            }
            result = (0,0);
            return false;
        }

        public void AddAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to,
            (double, double) tradeVolumes)
        {
            if (!IsCahedPeriod(from))
                return;

            if (!_assetPairVolumesCache.TryGetValue(clientId, out var clientDict))
            {
                var periodsDict = new ConcurrentDictionary<DateTime, (double, double)>();
                periodsDict.TryAdd(to, tradeVolumes);
                var assetDict = new ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, (double, double)>>();
                assetDict.TryAdd(from, periodsDict);
                clientDict = new ConcurrentDictionary<string, ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, (double, double)>>>();
                clientDict.TryAdd(assetPairId, assetDict);
                _assetPairVolumesCache.TryAdd(clientId, clientDict);
            }
            else if (!clientDict.TryGetValue(assetPairId, out var assetDict))
            {
                var periodsDict = new ConcurrentDictionary<DateTime, (double, double)>();
                periodsDict.TryAdd(to, tradeVolumes);
                assetDict = new ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, (double, double)>>();
                assetDict.TryAdd(from, periodsDict);
                clientDict.TryAdd(assetPairId, assetDict);
            }
            else if (!assetDict.TryGetValue(from, out var periodsDict))
            {
                periodsDict = new ConcurrentDictionary<DateTime, (double, double)>();
                periodsDict.TryAdd(to, tradeVolumes);
                assetDict.TryAdd(from, periodsDict);
            }
            else
            {
                periodsDict.TryAdd(to, tradeVolumes);
            }

            if (_assetPairVolumesCache.Count > 1000)
            {
                var now = DateTime.UtcNow;
                if (now.Subtract(_lastWarningTime).TotalMinutes >= 1)
                {
                    _log.WriteWarning(nameof(CachesManager), nameof(AddAssetPairTradeVolume), $"Already {_assetPairVolumesCache.Count} items in asset pair cache");
                    _lastWarningTime = now;
                }
            }
        }

        public void UpdateAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime time,
            (double, double) tradeVolumes)
        {
            if (!_assetPairVolumesCache.TryGetValue(clientId, out var clientDict))
                return;

            if (!clientDict.TryGetValue(assetPairId, out var assetDict))
                return;

            var periodStarts = new List<DateTime>(assetDict.Keys);
            foreach (var periodStart in periodStarts)
            {
                if (periodStart > time)
                    continue;

                if (!assetDict.TryGetValue(periodStart, out var periodsDict))
                    continue;

                var periodEnds = new List<DateTime>(periodsDict.Keys);
                foreach (var periodEnd in periodEnds)
                {
                    if (periodEnd < time)
                        continue;

                    if (!periodsDict.TryGetValue(periodEnd, out var volumes))
                        continue;

                    periodsDict.TryUpdate(periodEnd, (volumes.Item1 + tradeVolumes.Item1, volumes.Item2 + tradeVolumes.Item2), volumes);
                }
            }
        }

        private bool IsCahedPeriod(DateTime from)
        {
            DateTime now = DateTime.UtcNow.RoundToHour();
            var periodHoursLength = (int)now.Subtract(from).TotalHours;
            return periodHoursLength <= _cacheLifeHoursCount;
        }

        private void CleanUpCache<T>(ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<DateTime, ConcurrentDictionary<DateTime, T>>>> cache)
        {
            DateTime now = DateTime.UtcNow.RoundToHour();
            DateTime cacheStart = now.AddDays(-1);

            var clientsToRemove = new List<string>();
            var clients = new List<string>(cache.Keys);
            foreach (var client in clients)
            {
                if (!cache.TryGetValue(client, out var clientAssetsDict))
                    continue;

                var assetsToRemove = new List<string>();
                var clientAssets = new List<string>(clientAssetsDict.Keys);
                foreach (var clientAsset in clientAssets)
                {
                    if (!clientAssetsDict.TryGetValue(client, out var periodsDict))
                        continue;

                    var keysToRemove = new List<DateTime>();
                    var periodStarts = new List<DateTime>(periodsDict.Keys);
                    foreach (var periodStart in periodStarts)
                    {
                        if (periodStart >= cacheStart)
                            continue;

                        keysToRemove.Add(periodStart);
                    }
                    foreach (var key in keysToRemove)
                    {
                        periodsDict.TryRemove(key, out var _);
                    }
                    if (periodsDict.Count == 0)
                        assetsToRemove.Add(clientAsset);
                }
                foreach (var asset in assetsToRemove)
                {
                    clientAssetsDict.TryRemove(asset, out var _);
                }
                if (clientAssetsDict.Count == 0)
                    clientsToRemove.Add(client);
            }
            foreach (var client in clientsToRemove)
            {
                cache.Remove(client, out var _);
            }
        }
    }
}
