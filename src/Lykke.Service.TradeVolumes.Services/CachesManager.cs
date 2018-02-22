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
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, double>>> _assetVolumesCache
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, double>>>();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, (double, double)>>> _assetPairVolumesCache
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, (double, double)>>>();

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
            int periodKey = GetPeriodKey(from, to);
            if (IsCahedPeriod(from)
                && _assetVolumesCache.TryGetValue(clientId, out var clientDict)
                && clientDict.TryGetValue(assetId, out var assetDict)
                && assetDict.TryGetValue(periodKey, out result))
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
                clientDict = new ConcurrentDictionary<string, ConcurrentDictionary<int, double>>();
                _assetVolumesCache.TryAdd(clientId, clientDict);
            }
            clientDict = _assetVolumesCache[clientId];
            if (!clientDict.TryGetValue(assetId, out var assetDict))
            {
                assetDict = new ConcurrentDictionary<int, double>();
                clientDict.TryAdd(assetId, assetDict);
            }
            assetDict = clientDict[assetId];
            int periodKey = GetPeriodKey(from, to);
            assetDict.TryAdd(periodKey, tradeVolume);

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

            assetDict = clientDict[assetId];
            foreach (var pair in assetDict)
            {
                if (!IsInPeriod(time, pair.Key))
                    continue;

                assetDict[pair.Key] += tradeVolume;

                _log.WriteInfo(
                    nameof(CachesManager),
                    nameof(UpdateAssetTradeVolume),
                    $"Updated {assetId} cache for client {clientId} on {time} with {tradeVolume}");
            }
        }

        public bool TryGetAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to,
            out (double,double) result)
        {
            int periodKey = GetPeriodKey(from, to);
            if (IsCahedPeriod(from)
                && _assetPairVolumesCache.TryGetValue(clientId, out var clientDict)
                && clientDict.TryGetValue(assetPairId, out var assetPairDict)
                && assetPairDict.TryGetValue(periodKey, out result))
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
                clientDict = new ConcurrentDictionary<string, ConcurrentDictionary<int, (double, double)>>();
                _assetPairVolumesCache.TryAdd(clientId, clientDict);
            }
            clientDict = _assetPairVolumesCache[clientId];
            if (!clientDict.TryGetValue(assetPairId, out var assetPairDict))
            {
                assetPairDict = new ConcurrentDictionary<int, (double, double)>();
                clientDict.TryAdd(assetPairId, assetPairDict);
            }
            assetPairDict = clientDict[assetPairId];
            int periodKey = GetPeriodKey(from, to);
            assetPairDict.TryAdd(periodKey, tradeVolumes);

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
                if (!IsInPeriod(time, pair.Key))
                {
                    _log.WriteInfo(
                        nameof(CachesManager),
                        nameof(UpdateAssetPairTradeVolume),
                        $"Skipped {assetPairId} cache (key - {pair.Key}) for client {clientId} on {time} with ({tradeVolumes.Item1}, {tradeVolumes.Item2})");
                    continue;
                }

                assetPairDict[pair.Key] = tradeVolumes;

                _log.WriteInfo(
                    nameof(CachesManager),
                    nameof(UpdateAssetPairTradeVolume),
                    $"Updated {assetPairId} cache (key - {pair.Key}) for client {clientId} on {time} with ({tradeVolumes.Item1}, {tradeVolumes.Item2})");
            }
        }

        private bool IsCahedPeriod(DateTime from)
        {
            DateTime now = DateTime.UtcNow.RoundToHour();
            var periodHoursLength = (int)now.Subtract(from).TotalHours;
            return periodHoursLength <= _cacheLifeHoursCount;
        }

        private int GetPeriodKey(DateTime from, DateTime to)
        {
            var hoursLength = (int)to.Subtract(from).TotalHours;
            int key = CalculateKeyStart(from) + hoursLength;
            return key;
        }

        private int CalculateKeyStart(DateTime dateTime)
        {
            return ((((dateTime.Year % 100) * 13 // max number of months + 1
                + dateTime.Month) * 32 // max number of days + 1
                + dateTime.Day) * 25 // max number of hours + 1
                + dateTime.Hour) * (_cacheLifeHoursCount + 1);
        }

        private bool IsInPeriod(DateTime time, int periodKey)
        {
            int keyStart = CalculateKeyStart(time);
            int periodStart = periodKey - (periodKey % (_cacheLifeHoursCount + 1));
            return periodKey >= keyStart && keyStart <= periodKey;
        }

        private void CleanUpCache<T>(ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, T>>> cache)
        {
            DateTime now = DateTime.UtcNow.RoundToHour();
            DateTime cacheStart = now.AddDays(-1);
            int periodKey = GetPeriodKey(cacheStart, cacheStart);

            var clientsToRemove = new List<string>();
            foreach (var clientPair in cache)
            {
                var assetsToRemove = new List<string>();
                foreach (var assetPair in clientPair.Value)
                {
                    var keysToClear = new List<int>();
                    foreach (var item in assetPair.Value)
                    {
                        if (item.Key < periodKey)
                            keysToClear.Add(item.Key);
                    }
                    foreach (var key in keysToClear)
                    {
                        assetPair.Value.Remove(key, out var _);
                    }
                    if (assetPair.Value.Count == 0)
                        assetsToRemove.Add(assetPair.Key);
                }
                foreach (var asset in assetsToRemove)
                {
                    clientPair.Value.Remove(asset, out var _);
                }
                if (clientPair.Value.Count == 0)
                    clientsToRemove.Add(clientPair.Key);
            }
            foreach (var client in clientsToRemove)
            {
                cache.Remove(client, out var _);
            }
        }
    }
}
