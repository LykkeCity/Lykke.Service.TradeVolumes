using System;
using System.Linq;
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
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<DateTime, List<(DateTime, double)>>>> _assetVolumesCache
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<DateTime, List<(DateTime, double)>>>>();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<DateTime, List<(DateTime, (double, double))>>>> _assetPairVolumesCache
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<DateTime, List<(DateTime, (double, double))>>>>();

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
                && periods.Any(p => p.Item1 == to))
            {
                result = periods.First(p => p.Item1 == to).Item2;
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
                clientDict = new ConcurrentDictionary<string, ConcurrentDictionary<DateTime, List<(DateTime, double)>>>();
                _assetVolumesCache.TryAdd(clientId, clientDict);
            }
            clientDict = _assetVolumesCache[clientId];
            if (!clientDict.TryGetValue(assetId, out var assetDict))
            {
                assetDict = new ConcurrentDictionary<DateTime, List<(DateTime, double)>>();
                clientDict.TryAdd(assetId, assetDict);
            }
            assetDict = clientDict[assetId];
            assetDict.TryAdd(from, new List<(DateTime, double)> { (to, tradeVolume) });

            if (_assetVolumesCache.Count > 1000)
                _log.WriteWarning(nameof(CachesManager), nameof(AddAssetTradeVolume), $"Already {_assetVolumesCache.Count} items in asset cache");
        }

        public void UpdateAssetTradeVolume(
            string clientId,
            string assetId,
            DateTime time,
            double tradeVolume)
        {
            if (!_assetVolumesCache.TryGetValue(clientId, out var clientDict))
                return;

            clientDict = _assetVolumesCache[clientId];
            if (!clientDict.TryGetValue(assetId, out var assetDict))
                return;

            foreach (var pair in assetDict)
            {
                if (pair.Key > time)
                    continue;

                var periods = pair.Value;
                for(int i = 0; i < periods.Count; ++i)
                {
                    var period = periods[i];
                    if (period.Item1 < time)
                        continue;

                    periods[i] = (period.Item1, period.Item2 + tradeVolume);

                    _log.WriteInfo(
                        nameof(CachesManager),
                        nameof(UpdateAssetTradeVolume),
                        $"Updated {assetId} cache for client {clientId} on {time} with {tradeVolume}");
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
                && assetPairDict.TryGetValue(from, out var periods)
                && periods.Any(p => p.Item1 == to))
            {
                result = periods.First(p => p.Item1 == to).Item2;
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
                clientDict = new ConcurrentDictionary<string, ConcurrentDictionary<DateTime, List<(DateTime, (double, double))>>>();
                _assetPairVolumesCache.TryAdd(clientId, clientDict);
            }
            clientDict = _assetPairVolumesCache[clientId];
            if (!clientDict.TryGetValue(assetPairId, out var assetPairDict))
            {
                assetPairDict = new ConcurrentDictionary<DateTime, List<(DateTime, (double, double))>>();
                clientDict.TryAdd(assetPairId, assetPairDict);
            }
            assetPairDict = clientDict[assetPairId];
            assetPairDict.TryAdd(from, new List<(DateTime, (double, double))> { (to, tradeVolumes) });

            if (_assetPairVolumesCache.Count > 1000)
                _log.WriteWarning(nameof(CachesManager), nameof(AddAssetPairTradeVolume), $"Already {_assetPairVolumesCache.Count} items in asset pair cache");
        }

        public void UpdateAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime time,
            (double, double) tradeVolumes)
        {
            if (!_assetPairVolumesCache.TryGetValue(clientId, out var clientDict))
                return;

            clientDict = _assetPairVolumesCache[clientId];
            if (!clientDict.TryGetValue(assetPairId, out var assetPairDict))
                return;

            assetPairDict = clientDict[assetPairId];
            foreach (var pair in assetPairDict)
            {
                if (pair.Key > time)
                    continue;

                var periods = pair.Value;
                for (int i = 0; i < periods.Count; ++i)
                {
                    var period = periods[i];
                    if (period.Item1 < time)
                        continue;

                    periods[i] = (period.Item1, tradeVolumes);

                    _log.WriteInfo(
                        nameof(CachesManager),
                        nameof(UpdateAssetPairTradeVolume),
                        $"Updated {assetPairId} cache (key - {pair.Key}) for client {clientId} on {time} with ({tradeVolumes.Item1}, {tradeVolumes.Item2})");
                }
            }
        }

        private bool IsCahedPeriod(DateTime from)
        {
            DateTime now = DateTime.UtcNow.RoundToHour();
            var periodHoursLength = (int)now.Subtract(from).TotalHours;
            return periodHoursLength <= _cacheLifeHoursCount;
        }

        private void CleanUpCache<T>(ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<DateTime, List<(DateTime, T)>>>> cache)
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
                        _log.WriteInfo(
                            nameof(CachesManager),
                            nameof(UpdateAssetPairTradeVolume),
                            $"Cleanud up cache key {periodStart} for cache start {cacheStart}");
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
