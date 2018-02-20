using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Service.TradeVolumes.Core.Services;

namespace Lykke.Service.TradeVolumes.Services
{
    public class CachesManager : TimerPeriod, ICachesManager
    {
        private const int _cacheLifeHoursCount = 24 * 31;

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, double>>> _assetVolumesCache
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, double>>>();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, (double, double)>>> _assetPairVolumesCache
            = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, (double, double)>>>();
        private readonly ConcurrentDictionary<string, DateTime> _clearedIds = new ConcurrentDictionary<string, DateTime>();

        public CachesManager(ILog log)
            : base((int)TimeSpan.FromHours(1).TotalMilliseconds, log)
        {
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
                return true;
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
            if (_clearedIds.ContainsKey(clientId))
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
                return true;
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
            if (_clearedIds.ContainsKey(clientId))
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
        }

        public void ClearClientCache(string clientId)
        {
            _assetVolumesCache.TryRemove(clientId, out _);
            _assetPairVolumesCache.TryRemove(clientId, out _);
            _clearedIds.TryAdd(clientId, DateTime.UtcNow);
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
            int key = ((((from.Year % 100) * 13 // max number of months + 1
                + from.Month) * 32 // max number of days + 1
                + from.Day) * 25 // max number of hours + 1
                + from.Hour) * (_cacheLifeHoursCount + 1)
                + hoursLength;
            return key;
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

            var idsToCheck = _clearedIds.Keys.ToList();
            foreach (var id in idsToCheck)
            {
                var idTime = _clearedIds[id];
                if (now.Subtract(idTime).TotalMinutes > 15)
                    _clearedIds.TryRemove(id, out _);
            }
        }
    }
}
