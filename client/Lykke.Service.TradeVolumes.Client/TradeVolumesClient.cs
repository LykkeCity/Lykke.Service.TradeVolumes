using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Service.TradeVolumes.Client.AutorestClient;
using Lykke.Service.TradeVolumes.Client.Models;

namespace Lykke.Service.TradeVolumes.Client
{
    public class TradeVolumesClient : ITradeVolumesClient, IDisposable
    {
        private TradeVolumesAPI _service;

        public TradeVolumesClient(string serviceUrl)
        {
            if (string.IsNullOrWhiteSpace(serviceUrl))
                throw new ArgumentNullException(nameof(serviceUrl));

            _service = new TradeVolumesAPI(new Uri(serviceUrl));
        }

        public void Dispose()
        {
            if (_service == null)
                return;
            _service.Dispose();
            _service = null;
        }

        public async Task<AssetTradeVolumeResponse> GetAssetTradeVolumeAsync(string assetId, DateTime fromDate, DateTime toDate)
        {
            var result = await _service.GetPeriodAssetTradeVolumeAsync(
                assetId,
                fromDate,
                toDate);
            return new AssetTradeVolumeResponse
            {
                AssetId = result.AssetId,
                Volume = result.Volume,
            };
        }

        public async Task<AssetPairTradeVolumeResponse> GetAssetPairTradeVolumeAsync(string assetPairId, DateTime fromDate, DateTime toDate)
        {
            var result = await _service.GetPeriodAssetPairTradeVolumeAsync(
                assetPairId,
                fromDate,
                toDate);
            return new AssetPairTradeVolumeResponse
            {
                AssetPairId = result.AssetPairId,
                BaseVolume = result.BaseVolume,
                QuotingVolume = result.QuotingVolume,
            };
        }

        public async Task<List<AssetPairTradeVolumeResponse>> GetAssetPairsTradeVolumeAsync(string[] assetPairIds, DateTime fromDate, DateTime toDate)
        {
            var volumes = await _service.GetPeriodAssetPairsTradeVolumeAsync(
                fromDate,
                toDate,
                assetPairIds);

            var result = new List<AssetPairTradeVolumeResponse>();
            
            foreach (var volume in volumes)
            {
                result.Add(new AssetPairTradeVolumeResponse
                {
                    AssetPairId = volume.AssetPairId,
                    BaseVolume = volume.BaseVolume,
                    QuotingVolume = volume.QuotingVolume,
                });
            }

            return result;
        }

        public async Task<AssetTradeVolumeResponse> GetClientAssetTradeVolumeAsync(string assetId, string clientId, DateTime fromDate, DateTime toDate)
        {
            var result = await _service.GetPeriodClientAssetTradeVolumeAsync(
                assetId,
                clientId,
                fromDate,
                toDate);
            return new AssetTradeVolumeResponse
            {
                AssetId = result.AssetId,
                ClientId = result.ClientId,
                Volume = result.Volume,
            };
        }

        public async Task<AssetPairTradeVolumeResponse> GetClientAssetPairTradeVolumeAsync(string assetPairId, string clientId, DateTime fromDate, DateTime toDate)
        {
            var result = await _service.GetPeriodClientAssetPairTradeVolumeAsync(
                assetPairId,
                clientId,
                fromDate,
                toDate);
            return new AssetPairTradeVolumeResponse
            {
                AssetPairId = result.AssetPairId,
                ClientId = result.ClientId,
                BaseVolume = result.BaseVolume,
                QuotingVolume = result.QuotingVolume,
            };
        }

        public async Task<AssetTradeVolumeResponse> GetWalletAssetTradeVolumeAsync(string assetId, string walletId, DateTime fromDate, DateTime toDate)
        {
            var result = await _service.GetPeriodWalletAssetTradeVolumeAsync(
                assetId,
                walletId,
                fromDate,
                toDate);
            return new AssetTradeVolumeResponse
            {
                AssetId = result.AssetId,
                WalletId = result.WalletId,
                Volume = result.Volume,
            };
        }

        public async Task<AssetPairTradeVolumeResponse> GetWalletAssetPairTradeVolumeAsync(string assetPairId, string walletId, DateTime fromDate, DateTime toDate)
        {
            var result = await _service.GetPeriodWalletAssetPairTradeVolumeAsync(
                assetPairId,
                walletId,
                fromDate,
                toDate);
            return new AssetPairTradeVolumeResponse
            {
                AssetPairId = result.AssetPairId,
                WalletId = result.WalletId,
                BaseVolume = result.BaseVolume,
                QuotingVolume = result.QuotingVolume,
            };
        }
    }
}
