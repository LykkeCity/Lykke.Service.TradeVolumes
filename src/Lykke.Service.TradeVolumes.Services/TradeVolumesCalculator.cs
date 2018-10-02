using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Common.Log;
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
        private readonly TimeSpan _warningDelay;

        private DateTime? _lastProcessedDate;

        public TradeVolumesCalculator(
            IAssetsDictionary assetsDictionary,
            ICachesManager cachesManager,
            ITradeVolumesRepository tradeVolumesRepository,
            TimeSpan? warningDelay,
            ILog log)
        {
            _assetsDictionary = assetsDictionary;
            _tradeVolumesRepository = tradeVolumesRepository;
            _log = log;
            _cachesManager = cachesManager;
            _warningDelay = warningDelay ?? TimeSpan.FromMinutes(60);
        }

        public TradeVolumesCalculator(
            IAssetsDictionary assetsDictionary,
            ICachesManager cachesManager,
            ITradeVolumesRepository tradeVolumesRepository,
            TimeSpan? warningDelay,
            ILogFactory logFactory)
        {
            _assetsDictionary = assetsDictionary;
            _tradeVolumesRepository = tradeVolumesRepository;
            _log = logFactory.CreateLog(this);
            _cachesManager = cachesManager;
            _warningDelay = warningDelay ?? TimeSpan.FromMinutes(60);
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
                var processedItemsDict = new Dictionary<string, string>();
                var byAsset = hourGroup.GroupBy(i => i.Asset);
                foreach (var assetGroup in byAsset)
                {
                    var byOppositeAsset = assetGroup.GroupBy(i => i.OppositeAsset);
                    foreach (var oppositeAssetGroup in byOppositeAsset)
                    {
                        var assetPairId = await _assetsDictionary.GetAssetPairIdAsync(assetGroup.Key, oppositeAssetGroup.Key);
                        var (baseAssetId, _) = await _assetsDictionary.GetAssetIdsAsync(assetPairId);

                        var groupTime = oppositeAssetGroup.Max(i => i.DateTime);
                        var usersHash = new HashSet<string>(items.Count);
                        var walletsHash = new HashSet<string>(items.Count);
                        foreach (var item in oppositeAssetGroup)
                        {
                            usersHash.Add(item.UserId);
                            walletsHash.Add(item.WalletId);

                            if (processedItemsDict.ContainsKey(item.TradeId) && processedItemsDict[item.TradeId] == item.WalletId) //to avoid double counting
                                continue;

                            await UpdateCachedVolumesAsync(
                                assetPairId,
                                assetGroup.Key,
                                oppositeAssetGroup.Key,
                                baseAssetId == assetGroup.Key,
                                item.UserId,
                                item.WalletId,
                                item.TradeId,
                                groupTime,
                                baseAssetId == assetGroup.Key
                                    ? ((double)item.Volume, (double)item.OppositeVolume)
                                    : ((double)item.OppositeVolume, (double)item.Volume));

                            processedItemsDict[item.TradeId] = item.WalletId;
                        }

                        var volumesDict = await GetCurrentVolumes(
                            groupTime,
                            usersHash,
                            walletsHash,
                            assetGroup.Key,
                            oppositeAssetGroup.Key,
                            assetPairId,
                            baseAssetId == assetGroup.Key);

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
                if (missingDelay >= _warningDelay)
                    _log.WriteWarning(nameof(AddTradeLogItemsAsync), null, $"Tradelog items are missing for {missingDelay.TotalMinutes} minutes");
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
            var lastProcessedDate = _lastProcessedDate?.RoundToHour() ?? DateTime.UtcNow.RoundToHour();
            if (lastProcessedDate < to)
                to = lastProcessedDate.AddHours(1);

            double baseResult = 0;
            double quotingReult = 0;
            var cachedFrom = await _cachesManager.GetFirstCachedTimestampAsync(
                assetPairId,
                clientId,
                from,
                to,
                isUser);
            cachedFrom = cachedFrom.RoundToHour();
            if (cachedFrom < to)
            {
                var cachedVolumes = await _cachesManager.GetAssetPairTradeVolumeAsync(
                    assetPairId,
                    clientId,
                    from,
                    to,
                    isUser);
                if (cachedVolumes.Item1.HasValue && cachedVolumes.Item2.HasValue)
                {
                    baseResult += cachedVolumes.Item1.Value;
                    quotingReult += cachedVolumes.Item2.Value;
                    to = cachedFrom.AddHours(-1);
                }
            }

            if (to > from)
            {
                var (baseAssetId, quotingAssetId) = await _assetsDictionary.GetAssetIdsAsync(assetPairId);
                var (baseVolume, quotingVolume) = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                    baseAssetId,
                    quotingAssetId,
                    clientId,
                    from,
                    to,
                    isUser);
                baseResult += baseVolume;
                quotingReult += quotingVolume;
            }

            return (baseResult, quotingReult);
        }

        public async Task<double> GetPeriodAssetVolumeAsync(
            string assetId,
            string clientId,
            DateTime from,
            DateTime to,
            bool isUser)
        {
            var lastProcessedDate = _lastProcessedDate?.RoundToHour() ?? DateTime.UtcNow.RoundToHour();
            if (lastProcessedDate < to)
                to = lastProcessedDate.AddHours(1);

            (double result, _) = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                assetId,
                null,
                clientId,
                from,
                to,
                isUser);

            return result;
        }

        private async Task UpdateCachedVolumesAsync(
            string assetPairId,
            string assetId,
            string oppositeAssetId,
            bool isBaseAsset,
            string userId,
            string walletId,
            string tradeId,
            DateTime timestamp,
            (double, double) tradeVolumes)
        {
            var hourStart = timestamp.RoundToHour();
            if (timestamp > hourStart)
            {
                var cachedVolumes = await _cachesManager.GetAssetPairTradeVolumeAsync(
                    assetPairId,
                    userId,
                    DateTime.MinValue,
                    timestamp,
                    true);
                if (!cachedVolumes.Item1.HasValue || !cachedVolumes.Item2.HasValue)
                {
                    var hourVolumes = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                        isBaseAsset ? assetId : oppositeAssetId,
                        isBaseAsset ? oppositeAssetId : assetPairId,
                        userId,
                        hourStart,
                        timestamp,
                        true);
                    await _cachesManager.AddAssetPairTradeVolumeAsync(
                        assetPairId,
                        userId,
                        walletId,
                        $"CacheWarmupBefore-{tradeId}",
                        timestamp.AddTicks(-1),
                        hourVolumes);
                }

                cachedVolumes = await _cachesManager.GetAssetPairTradeVolumeAsync(
                    assetPairId,
                    walletId,
                    DateTime.MinValue,
                    timestamp,
                    false);
                if (!cachedVolumes.Item1.HasValue || !cachedVolumes.Item2.HasValue)
                {
                    var hourVolumes = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                        isBaseAsset ? assetId : oppositeAssetId,
                        isBaseAsset ? oppositeAssetId : assetPairId,
                        walletId,
                        hourStart,
                        timestamp,
                        false);
                    await _cachesManager.AddAssetPairTradeVolumeAsync(
                        assetPairId,
                        userId,
                        walletId,
                        $"CacheWarmupBefore-{tradeId}",
                        timestamp.AddTicks(-1),
                        hourVolumes);
                }
            }

            await _cachesManager.AddAssetPairTradeVolumeAsync(
                assetPairId,
                userId,
                walletId,
                tradeId,
                timestamp,
                tradeVolumes);
        }

        private async Task<Dictionary<string, double[]>> GetCurrentVolumes(
            DateTime timestamp,
            ICollection<string> userIds,
            ICollection<string> walletIds,
            string assetId,
            string oppositeAssetId,
            string assetPairId,
            bool isBaseAsset)
        {
            var result = new Dictionary<string, double[]>(userIds.Count + walletIds.Count);
            var hourStart = timestamp.RoundToHour();
            var hourEnd = hourStart.AddHours(1);

            var usersToLoad = new List<string>();
            foreach (var userId in userIds)
            {
                var cachedVolumes = await _cachesManager.GetAssetPairTradeVolumeAsync(
                    assetPairId,
                    userId,
                    hourStart,
                    hourEnd,
                    true);
                if (cachedVolumes.Item1.HasValue && cachedVolumes.Item2.HasValue)
                    result[userId] = isBaseAsset
                        ? new [] { cachedVolumes.Item1.Value, cachedVolumes.Item2.Value }
                        : new [] { cachedVolumes.Item2.Value, cachedVolumes.Item1.Value };
                else
                    usersToLoad.Add(userId);
            }

            var walletsToLoad = new List<string>();
            foreach (var walletId in walletIds)
            {
                var cachedVolumes = await _cachesManager.GetAssetPairTradeVolumeAsync(
                    assetPairId,
                    walletId,
                    hourStart,
                    hourEnd,
                    false);
                if (cachedVolumes.Item1.HasValue && cachedVolumes.Item2.HasValue)
                    result[walletId] = isBaseAsset
                        ? new[] { cachedVolumes.Item1.Value, cachedVolumes.Item2.Value }
                        : new[] { cachedVolumes.Item2.Value, cachedVolumes.Item1.Value };
                else
                    walletsToLoad.Add(walletId);
            }

            if (usersToLoad.Count > 0 || walletsToLoad.Count > 0)
            {
                var tableData = await _tradeVolumesRepository.GetUserWalletsTradeVolumesAsync(
                    timestamp,
                    usersToLoad,
                    walletsToLoad,
                    assetId,
                    oppositeAssetId);
                foreach (var pair in tableData)
                {
                    result[pair.Key] = pair.Value;
                }
            }

            return result;
        }
    }
}
