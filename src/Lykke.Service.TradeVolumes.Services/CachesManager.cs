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
        private const string _userAssetPairTradesSetKeyPattern = "TradeVolumes:volumes:assetPairId:{0}:userId:{1}";
        private const string _walletAssetPairTradesSetKeyPattern = "TradeVolumes:volumes:assetPairId:{0}:walletId:{1}";
        private const string _tradeKeySuffixPattern = "ticks:{0}";
        private const string _tradeIdAssetPairSetKeyPattern = "TradeVolumes:tradeIdHash:tradeId:{0}:userId:{1}";

        private readonly IDatabase _db;

        public CachesManager(IConnectionMultiplexer connectionMultiplexer)
        {
            _db = connectionMultiplexer.GetDatabase();
        }

        public async Task<(double?, double?)> GetAssetPairTradeVolumeAsync(
            string assetPairId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser)
        {
            if (!IsCahedPeriod(from))
                return (null, null);

            var setKey = string.Format(
                isUser ? _userAssetPairTradesSetKeyPattern : _walletAssetPairTradesSetKeyPattern,
                assetPairId,
                clientId);

            var keys = await GetSetKeysAsync(
                setKey,
                from,
                to);
            if (keys == null || keys.Length == 0)
                return (null, null);

            var trades = await _db.StringGetAsync(keys);
            var tradeVolumesStr = trades.Where(i => i.HasValue);
            if (!tradeVolumesStr.Any())
                return (null, null);

            double baseResult = 0;
            double quotingResult = 0;
            var tradeVolumes = tradeVolumesStr.Select(i => i.ToString().DeserializeJson<CacheTradeVolumeModel>());
            foreach (var tradeVolume in tradeVolumes)
            {
                baseResult += tradeVolume.BaseVolume;
                quotingResult += tradeVolume.QuotingVolume;
            }
            return (baseResult, quotingResult);
        }

        public async Task AddAssetPairTradeVolumeAsync(
            string assetPairId,
            string userId,
            string walletId,
            string tradeId,
            DateTime time,
            (double, double) tradeVolumes)
        {
            if (!IsCahedPeriod(time))
                return;

            var tradeIdSetKey = string.Format(_tradeIdAssetPairSetKeyPattern, tradeId, userId);
            var tradeWallets = await _db.SetMembersAsync(tradeIdSetKey);
            if (tradeWallets != null && tradeWallets.Length > 0)
            {
                await _db.KeyDeleteAsync(tradeIdSetKey);
                // trading with other wallet of the same user is not included
                if (tradeWallets.Select(w => w.ToString()).Any(w => w == walletId))
                {
                    var userKey = string.Format(_userAssetPairTradesSetKeyPattern, assetPairId, userId);
                    var walletKey = string.Format(_walletAssetPairTradesSetKeyPattern, assetPairId, walletId);
                    var suffix = string.Format(_tradeKeySuffixPattern, time.Ticks);
                    await _db.KeyDeleteAsync(new RedisKey[]
                    {
                        $"{userKey}:{suffix}",
                        $"{walletKey}:{suffix}",
                    });
                    await _db.SortedSetRemoveAsync(userKey, suffix);
                    await _db.SortedSetRemoveAsync(walletKey, suffix);
                    return; 
                }
            }

            await _db.SetAddAsync(tradeIdSetKey, walletId);
            await _db.KeyExpireAsync(tradeIdSetKey, TimeSpan.FromMinutes(60));

            var tradeVolume = new CacheTradeVolumeModel
            {
                BaseVolume = Math.Abs(tradeVolumes.Item1),
                QuotingVolume = Math.Abs(tradeVolumes.Item2),
            };
            string tradeVolumeJson = tradeVolume.ToJson();
            var tradeKeySuffix = string.Format(_tradeKeySuffixPattern, time.Ticks);

            var tx = _db.CreateTransaction();
            var userSetKey = string.Format(_userAssetPairTradesSetKeyPattern, assetPairId, userId);
            var walletSetKey = string.Format(_walletAssetPairTradesSetKeyPattern, assetPairId, walletId);
            var tasks = new List<Task>
            {
                tx.SortedSetAddAsync(userSetKey, tradeKeySuffix, time.Ticks),
                tx.SortedSetAddAsync(walletSetKey, tradeKeySuffix, time.Ticks),
            };
            TimeSpan ttl = time.AddMonths(1).Subtract(DateTime.UtcNow);
            var userTradeKey = $"{userSetKey}:{tradeKeySuffix}";
            var userSetKeyTask = tx.StringSetAsync(userTradeKey, tradeVolumeJson, ttl);
            tasks.Add(userSetKeyTask);
            var walletTradeKey = $"{walletSetKey}:{tradeKeySuffix}";
            var walletSetKeyTask = tx.StringSetAsync(walletTradeKey, tradeVolumeJson, ttl);
            tasks.Add(walletSetKeyTask);

            if (!await tx.ExecuteAsync())
                throw new InvalidOperationException($"Error during trade volume adding for wallet {walletId} on asset pair {assetPairId}");
            await Task.WhenAll(tasks);
            if (!userSetKeyTask.Result)
                throw new InvalidOperationException($"Error during trade volume adding for user {userId} on asset pair {assetPairId}");
            if (!walletSetKeyTask.Result)
                throw new InvalidOperationException($"Error during trade volume adding for wallet {walletId} on asset pair {assetPairId}");
        }

        public async Task<DateTime> GetFirstCachedTimestampAsync(
            string assetPairId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser)
        {
            if (!IsCahedPeriod(from))
                return to;

            var setKey = string.Format(
                isUser ? _userAssetPairTradesSetKeyPattern : _walletAssetPairTradesSetKeyPattern,
                assetPairId,
                clientId);

            var setItems = await _db.SortedSetRangeByScoreAsync(setKey, from.Ticks, to.Ticks);
            var resultItem = setItems?.First(i => i.HasValue);
            if (resultItem == null)
                return to;

            var parts = resultItem.ToString().Split(':', StringSplitOptions.RemoveEmptyEntries);
            long ticks = long.Parse(parts[parts.Length - 1]);
            return new DateTime(ticks);
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

            return getKeysTask.Result?
                .Select(i => (RedisKey)$"{setKey}:{i.ToString()}")
                .ToArray();
        }
    }
}
