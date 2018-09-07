using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Services.Models;
using StackExchange.Redis;

namespace Lykke.Service.TradeVolumes.Services
{
    public class CachesManager : ICachesManager
    {
        private const string _clientAssetTradesSetKeyPattern = "TradeVolumes:volumes:walletId:{0}:assetId:{1}";
        private const string _clientAssetPairTradesSetKeyPattern = "TradeVolumes:volumes:walletId:{0}:assetPairId:{1}";
        private const string _tradeKeySuffixPattern = "ticks:{0}";

        private readonly IDatabase _db;

        public CachesManager(IConnectionMultiplexer connectionMultiplexer)
        {
            _db = connectionMultiplexer.GetDatabase();
        }

        public async Task<double?> GetAssetTradeVolumeAsync(
            string clientId,
            string assetId,
            DateTime from,
            DateTime to)
        {
            if (!IsCahedPeriod(from))
                return null;

            var keys = await GetSetKeysAsync(
                string.Format(_clientAssetTradesSetKeyPattern, clientId, assetId),
                from,
                to);
            if (keys.Length == 0)
                return null;

            var trades = await _db.StringGetAsync(keys);
            var tradeVolumes = trades
                .Where(i => i.HasValue)
                .Select(i => double.Parse(i));

            if (tradeVolumes.Any())
                return tradeVolumes.Sum();
            return null;
        }

        public async Task AddAssetTradeVolumeAsync(
            string clientId,
            string assetId,
            DateTime time,
            double tradeVolume)
        {
            if (!IsCahedPeriod(time))
                return;

            string setKey = string.Format(_clientAssetTradesSetKeyPattern, clientId, assetId);
            var tradeKeySuffix = string.Format(_tradeKeySuffixPattern, time.Ticks);
            var tradeKey = $"{setKey}:{tradeKeySuffix}";

            var tx = _db.CreateTransaction();
            var tasks = new List<Task>
            {
                tx.SortedSetAddAsync(setKey, tradeKeySuffix, time.Ticks)
            };
            var setKeyTask = tx.StringSetAsync(tradeKey, tradeVolume.ToString(), time.AddMonths(1).Subtract(DateTime.UtcNow));
            tasks.Add(setKeyTask);
            if (!await tx.ExecuteAsync())
                throw new InvalidOperationException($"Error during trade volune adding for client {clientId} on asset {assetId}");
            await Task.WhenAll(tasks);
            if (!setKeyTask.Result)
                throw new InvalidOperationException($"Error during trade volune adding for client {clientId} on asset {assetId}");
        }

        public async Task<(double?, double?)> GetAssetPairTradeVolumeAsync(
            string clientId,
            string assetPairId,
            DateTime from,
            DateTime to)
        {
            if (!IsCahedPeriod(from))
                return (null, null);

            var keys = await GetSetKeysAsync(
                string.Format(_clientAssetPairTradesSetKeyPattern, clientId, assetPairId),
                from,
                to);
            if (keys.Length == 0)
                return (null, null);

            var trades = await _db.StringGetAsync(keys);
            var tradeVolumes = trades
                .Where(i => i.HasValue)
                .Select(i => i.ToString().DeserializeJson<CacheTradeVolumeModel>());
            if (tradeVolumes.Any())
            {
                double baseResult = 0;
                double quotingResult = 0;
                foreach (var tradeVolume in tradeVolumes)
                {
                    baseResult += tradeVolume.BaseVolume;
                    quotingResult += tradeVolume.QuotingVolume;
                }
                return (baseResult, quotingResult);
            }
            return (null, null);
        }

        public async Task AddAssetPairTradeVolumeAsync(
            string clientId,
            string assetPairId,
            DateTime time,
            (double, double) tradeVolumes)
        {
            if (!IsCahedPeriod(time))
                return;

            string setKey = string.Format(_clientAssetPairTradesSetKeyPattern, clientId, assetPairId);
            var tradeKeySuffix = string.Format(_tradeKeySuffixPattern, time.Ticks);
            var tradeKey = $"{setKey}:{tradeKeySuffix}";
            var tradeVolume = new CacheTradeVolumeModel
            {
                BaseVolume = tradeVolumes.Item1,
                QuotingVolume = tradeVolumes.Item2,
            };

            var tx = _db.CreateTransaction();
            var tasks = new List<Task>
            {
                tx.SortedSetAddAsync(setKey, tradeKeySuffix, time.Ticks)
            };
            var setKeyTask = tx.StringSetAsync(tradeKey, tradeVolume.ToJson(), time.AddMonths(1).Subtract(DateTime.UtcNow));
            tasks.Add(setKeyTask);
            if (!await tx.ExecuteAsync())
                throw new InvalidOperationException($"Error during trade volune adding for client {clientId} on asset pair {assetPairId}");
            await Task.WhenAll(tasks);
            if (!setKeyTask.Result)
                throw new InvalidOperationException($"Error during trade volune adding for client {clientId} on asset pair {assetPairId}");
        }

        private bool IsCahedPeriod(DateTime from)
        {
            return from >= DateTime.UtcNow.RoundToHour().AddMonths(-1);
        }

        private async Task<RedisKey[]> GetSetKeysAsync(
            string setKey,
            DateTime from,
            DateTime to)
        {
            var actualPeriodStartScore = DateTime.UtcNow.AddMonths(-1).Ticks;
            var startScore = actualPeriodStartScore > from.Ticks ? actualPeriodStartScore : from.Ticks;
            var tx = _db.CreateTransaction();
            tx.AddCondition(Condition.KeyExists(setKey));
            var tasks = new List<Task>
            {
                tx.SortedSetRemoveRangeByScoreAsync(setKey, 0, actualPeriodStartScore)
            };
            var getKeysTask = tx.SortedSetRangeByScoreAsync(setKey, startScore, to.Ticks);
            tasks.Add(getKeysTask);
            if (await tx.ExecuteAsync())
                await Task.WhenAll(tasks);
            else
                return new RedisKey[0];

            return getKeysTask.Result
                .Select(i => (RedisKey)$"{setKey}:{i.ToString()}")
                .ToArray();
        }

    }
}
