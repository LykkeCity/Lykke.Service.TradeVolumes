using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.TradesConverter.Contract;
using Lykke.Service.TradeVolumes.Core;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Core.Repositories;
using StackExchange.Redis;

namespace Lykke.Service.TradeVolumes.Services
{
    public class TradeVolumesCalculator : ITradeVolumesCalculator
    {
        private const string _dateFormat = "yyyy-MM-dd HH:mm:ss.fff";
        private const string _userAssetTradeKeyPattern = "TradeVolumes:trades:tradeId:{0}:userId:{1}:assetId:{2}";

        private readonly IAssetsDictionary _assetsDictionary;
        private readonly ITradeVolumesRepository _tradeVolumesRepository;
        private readonly ILog _log;
        private readonly ICachesManager _cachesManager;
        private readonly TimeSpan _warningDelay;
        private readonly TimeSpan _cacheTimeout;
        private readonly IDatabase _db;

        private DateTime? _lastProcessedDate;

        public TradeVolumesCalculator(
            IAssetsDictionary assetsDictionary,
            ICachesManager cachesManager,
            ITradeVolumesRepository tradeVolumesRepository,
            IConnectionMultiplexer connectionMultiplexer,
            TimeSpan warningDelay,
            TimeSpan cacheTimeout,
            ILog log)
        {
            _assetsDictionary = assetsDictionary;
            _tradeVolumesRepository = tradeVolumesRepository;
            _log = log;
            _cachesManager = cachesManager;
            _warningDelay = warningDelay;
            _cacheTimeout = cacheTimeout;
            _db = connectionMultiplexer.GetDatabase();
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

            if (clientId != Constants.AllClients
                && to.Subtract(from).TotalDays <= Constants.MaxPeriodInDays)
            {
                var (cachedBaseResult, cachedQuotingResult) = await _cachesManager.GetAssetPairTradeVolumeAsync(
                    clientId,
                    assetPairId,
                    from,
                    to);
                if (cachedBaseResult.HasValue && cachedQuotingResult.HasValue)
                return (cachedBaseResult.Value, cachedQuotingResult.Value);
            }

            (string baseAssetId, string quotingAssetId) = await _assetsDictionary.GetAssetIdsAsync(assetPairId);
            (double baseVolume, double quotingVolume) = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                baseAssetId,
                quotingAssetId,
                clientId,
                from,
                to,
                isUser);
            var result = (baseVolume, quotingVolume);

            return result;
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

            if (clientId != Constants.AllClients && to.Subtract(from).TotalDays <= Constants.MaxPeriodInDays)
            {
                var cachedResult = await _cachesManager.GetAssetTradeVolumeAsync(
                    clientId,
                    assetId,
                    from,
                    to);
                if (cachedResult.HasValue)
                    return cachedResult.Value;
            }

            (double result, _) = await _tradeVolumesRepository.GetPeriodClientVolumeAsync(
                assetId,
                null,
                clientId,
                from,
                to,
                isUser);

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
            await _cachesManager.AddAssetTradeVolumeAsync(
                clientId,
                assetId,
                time,
                baseVolume);

            var assetPairId = await _assetsDictionary.GetAssetPairIdAsync(assetId, oppositeAssetId);
            (string baseAssetId, _) = await _assetsDictionary.GetAssetIdsAsync(assetPairId);
            if (assetId != baseAssetId)
                return;

            await _cachesManager.AddAssetPairTradeVolumeAsync(
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
            var userAssetTradeKey = string.Format(_userAssetTradeKeyPattern, item.TradeId, item.UserId, item.Asset);

            if (await _db.KeyExistsAsync(userAssetTradeKey))
            {
                userVolumes[0] -= (double) item.Volume;
                userVolumes[1] -= item.OppositeVolume.HasValue ? (double) item.OppositeVolume.Value : 0;
                await UpdateVolumesCacheAsync(
                    assetId,
                    oppositeAssetId,
                    item.UserId,
                    item.DateTime,
                    (double) -item.Volume,
                    item.OppositeVolume.HasValue ? (double) -item.OppositeVolume.Value : 0);

                await _db.KeyDeleteAsync(userAssetTradeKey);
            }
            else
            {
                userVolumes[0] += (double) item.Volume;
                userVolumes[1] += item.OppositeVolume.HasValue ? (double) item.OppositeVolume.Value : 0;
                await UpdateVolumesCacheAsync(
                    assetId,
                    oppositeAssetId,
                    item.UserId,
                    item.DateTime,
                    (double) item.Volume,
                    item.OppositeVolume.HasValue ? (double) item.OppositeVolume.Value : 0);

                await _db.StringSetAsync(userAssetTradeKey, item.DateTime.ToString(_dateFormat), _cacheTimeout);
            }
        }
    }
}
