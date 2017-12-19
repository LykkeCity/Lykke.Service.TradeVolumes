using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Common;
using Common.Log;

namespace Lykke.Service.TradeVolumes.Services
{
    internal class CachesManager : TimerPeriod
    {
        private const int _cacheLifeHoursCount = 24 * 31;

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, double>>> _assetVolumesCache
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, double>>>();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, (double, double)>>> _assetPairVolumesCache
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, (double, double)>>>();

        internal CachesManager(ILog log)
            : base((int)TimeSpan.FromHours(1).TotalMilliseconds, log)
        {
        }

        public override async Task Execute()
        {
            CleanUpCache(_assetVolumesCache);
            CleanUpCache(_assetPairVolumesCache);
        }

        private void CleanUpCache<T>(ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, T>>> cache)
        {
            DateTime now = DateTime.UtcNow.RoundToHour();
            DateTime oldestPossible = now.AddHours(-_cacheLifeHoursCount);
            int periodKey = GetPeriodKey(oldestPossible, oldestPossible);

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

        internal bool TryGetAssetTradeVolume(
            string clientId,
            string assetId,
            DateTime from,
            DateTime to,
            DateTime now,
            out double result)
        {
            int periodKey = GetPeriodKey(from, to);
            if (IsCahedPeriod(from, now)
                && _assetVolumesCache.TryGetValue(clientId, out var clientDict)
                && clientDict.TryGetValue(assetId, out var assetDict)
                && assetDict.TryGetValue(periodKey, out result))
                return true;
            result = 0;
            return false;
        }

        internal void AddAssetTradeVolume(
            string clientId,
            string assetId,
            DateTime from,
            DateTime to,
            DateTime now,
            double tradeVolume)
        {
            if (!IsCahedPeriod(from, now))
                return;
            if (!_assetVolumesCache.TryGetValue(clientId, out var clientDict))
            {
                clientDict = new ConcurrentDictionary<string, ConcurrentDictionary<int, double>>();
                _assetVolumesCache.TryAdd(clientId, clientDict);
            }
            if (!clientDict.TryGetValue(assetId, out var assetDict))
            {
                assetDict = new ConcurrentDictionary<int, double>();
                clientDict.TryAdd(assetId, assetDict);
            }
            int periodKey = GetPeriodKey(from, to);
            assetDict.TryAdd(periodKey, tradeVolume);
        }

        internal bool TryGetAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to,
            DateTime now,
            out (double,double) result)
        {
            int periodKey = GetPeriodKey(from, to);
            if (IsCahedPeriod(from, now)
                && _assetPairVolumesCache.TryGetValue(clientId, out var clientDict)
                && clientDict.TryGetValue(assetPairId, out var assetPairDict)
                && assetPairDict.TryGetValue(periodKey, out result))
                return true;
            result = (0,0);
            return false;
        }

        internal void AddAssetPairTradeVolume(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to,
            DateTime now,
            (double, double) tradeVolumes)
        {
            if (!IsCahedPeriod(from, now))
                return;
            if (!_assetPairVolumesCache.TryGetValue(clientId, out var clientDict))
            {
                clientDict = new ConcurrentDictionary<string, ConcurrentDictionary<int, (double, double)>>();
                _assetPairVolumesCache.TryAdd(clientId, clientDict);
            }
            if (!clientDict.TryGetValue(assetPairId, out var assetPairDict))
            {
                assetPairDict = new ConcurrentDictionary<int, (double, double)>();
                clientDict.TryAdd(assetPairId, assetPairDict);
            }
            int periodKey = GetPeriodKey(from, to);
            assetPairDict.TryAdd(periodKey, tradeVolumes);
        }

        private bool IsCahedPeriod(DateTime from, DateTime now)
        {
            var periodHoursLength = (int)now.Subtract(from).TotalHours;
            return periodHoursLength <= _cacheLifeHoursCount;
        }

        private int GetPeriodKey(DateTime from, DateTime to)
        {
            var hoursLength = (int)to.Subtract(from).TotalHours;
            int key = ((((from.Year % 100) * 13 + from.Month) * 32 + from.Day) * 25 + from.Hour) * (_cacheLifeHoursCount + 1) + hoursLength;
            return key;
        }
    }
}
