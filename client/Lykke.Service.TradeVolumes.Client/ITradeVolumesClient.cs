﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Service.TradeVolumes.Client.Models;

namespace Lykke.Service.TradeVolumes.Client
{
    /// <summary>
    /// ITradeVolumesClient interface.
    /// </summary>
    public interface ITradeVolumesClient
    {
        /// <summary>
        /// Get trade volume for specified asset.
        /// </summary>
        /// <param name="assetId">Asset id.</param>
        /// <param name="fromDate">Search from (inclusive) with hour precision.</param>
        /// <param name="toDate">Search to (exclusive) with hour precision.</param>
        /// <returns>Asset trade volume.</returns>
        [Obsolete("This method has quite poor performance.")]
        Task<AssetTradeVolumeResponse> GetAssetTradeVolumeAsync(
            string assetId,
            DateTime fromDate,
            DateTime toDate);

        /// <summary>
        /// Get trade volume for specified asset pair.
        /// </summary>
        /// <param name="assetPairId">Asset pair id.</param>
        /// <param name="fromDate">Search from (inclusive) with hour precision.</param>
        /// <param name="toDate">Search to (exclusive) with hour precision.</param>
        /// <returns>Asset pair trade volume.</returns>
        Task<AssetPairTradeVolumeResponse> GetAssetPairTradeVolumeAsync(
            string assetPairId,
            DateTime fromDate,
            DateTime toDate);

        /// <summary>
        /// Get trade volume for specified asset pairs.
        /// </summary>
        /// <param name="assetPairIds">Asset pair ids.</param>
        /// <param name="fromDate">Search from (inclusive) with hour precision.</param>
        /// <param name="toDate">Search to (exclusive) with hour precision.</param>
        /// <returns>Asset pair trade volume.</returns>
        Task<List<AssetPairTradeVolumeResponse>> GetAssetPairsTradeVolumeAsync(
            string[] assetPairIds,
            DateTime fromDate,
            DateTime toDate);

        /// <summary>
        /// Get client trade volume for specified asset.
        /// </summary>
        /// <param name="assetId">Asset id.</param>
        /// <param name="clientId">Client id.</param>
        /// <param name="fromDate">Search from (inclusive) with hour precision.</param>
        /// <param name="toDate">Search to (exclusive) with hour precision.</param>
        /// <returns>Asset trade volume for client.</returns>
        [Obsolete("This method has quite poor performance.")]
        Task<AssetTradeVolumeResponse> GetClientAssetTradeVolumeAsync(
            string assetId,
            string clientId,
            DateTime fromDate,
            DateTime toDate);

        /// <summary>
        /// Get client trade volume for specified asset pair.
        /// </summary>
        /// <param name="assetPairId">Asset pair id.</param>
        /// <param name="clientId">Client id.</param>
        /// <param name="fromDate">Search from (inclusive) with hour precision.</param>
        /// <param name="toDate">Search to (exclusive) with hour precision.</param>
        /// <returns>Asset pair trade volume for client.</returns>
        Task<AssetPairTradeVolumeResponse> GetClientAssetPairTradeVolumeAsync(
            string assetPairId,
            string clientId,
            DateTime fromDate,
            DateTime toDate);

        /// <summary>
        /// Get client trade volume for specified asset.
        /// </summary>
        /// <param name="assetId">Asset id.</param>
        /// <param name="walletId">Wallet id.</param>
        /// <param name="fromDate">Search from (inclusive) with hour precision.</param>
        /// <param name="toDate">Search to (exclusive) with hour precision.</param>
        /// <returns>Asset trade volume for client.</returns>
        [Obsolete("This method has quite poor performance.")]
        Task<AssetTradeVolumeResponse> GetWalletAssetTradeVolumeAsync(
            string assetId,
            string walletId,
            DateTime fromDate,
            DateTime toDate);

        /// <summary>
        /// Get client trade volume for specified asset pair.
        /// </summary>
        /// <param name="assetPairId">Asset pair id.</param>
        /// <param name="walletId">Wallet id.</param>
        /// <param name="fromDate">Search from (inclusive) with hour precision.</param>
        /// <param name="toDate">Search to (exclusive) with hour precision.</param>
        /// <returns>Asset pair trade volume for client.</returns>
        Task<AssetPairTradeVolumeResponse> GetWalletAssetPairTradeVolumeAsync(
            string assetPairId,
            string walletId,
            DateTime fromDate,
            DateTime toDate);
    }
}
