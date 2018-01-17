// Code generated by Microsoft (R) AutoRest Code Generator 1.1.0.0
// Changes may cause incorrect behavior and will be lost if the code is
// regenerated.

namespace Lykke.Service.TradeVolumes.Client.AutorestClient
{
    using Lykke.Service;
    using Lykke.Service.TradeVolumes;
    using Lykke.Service.TradeVolumes.Client;
    using Models;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Extension methods for TradeVolumesAPI.
    /// </summary>
    public static partial class TradeVolumesAPIExtensions
    {
            /// <summary>
            /// Checks service is alive
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            public static object IsAlive(this ITradeVolumesAPI operations)
            {
                return operations.IsAliveAsync().GetAwaiter().GetResult();
            }

            /// <summary>
            /// Checks service is alive
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='cancellationToken'>
            /// The cancellation token.
            /// </param>
            public static async Task<object> IsAliveAsync(this ITradeVolumesAPI operations, CancellationToken cancellationToken = default(CancellationToken))
            {
                using (var _result = await operations.IsAliveWithHttpMessagesAsync(null, cancellationToken).ConfigureAwait(false))
                {
                    return _result.Body;
                }
            }

            /// <summary>
            /// Calculates trade volume of assetId within specified time period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetId'>
            /// Asset Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            public static AssetTradeVolumeResponse GetPeriodAssetTradeVolume(this ITradeVolumesAPI operations, string assetId, System.DateTime fromDate, System.DateTime toDate)
            {
                return operations.GetPeriodAssetTradeVolumeAsync(assetId, fromDate, toDate).GetAwaiter().GetResult();
            }

            /// <summary>
            /// Calculates trade volume of assetId within specified time period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetId'>
            /// Asset Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            /// <param name='cancellationToken'>
            /// The cancellation token.
            /// </param>
            public static async Task<AssetTradeVolumeResponse> GetPeriodAssetTradeVolumeAsync(this ITradeVolumesAPI operations, string assetId, System.DateTime fromDate, System.DateTime toDate, CancellationToken cancellationToken = default(CancellationToken))
            {
                using (var _result = await operations.GetPeriodAssetTradeVolumeWithHttpMessagesAsync(assetId, fromDate, toDate, null, cancellationToken).ConfigureAwait(false))
                {
                    return _result.Body;
                }
            }

            /// <summary>
            /// Calculates trade volume of assetPairId within specified time period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetPairId'>
            /// AssetPair Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            public static AssetPairTradeVolumeResponse GetPeriodAssetPairTradeVolume(this ITradeVolumesAPI operations, string assetPairId, System.DateTime fromDate, System.DateTime toDate)
            {
                return operations.GetPeriodAssetPairTradeVolumeAsync(assetPairId, fromDate, toDate).GetAwaiter().GetResult();
            }

            /// <summary>
            /// Calculates trade volume of assetPairId within specified time period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetPairId'>
            /// AssetPair Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            /// <param name='cancellationToken'>
            /// The cancellation token.
            /// </param>
            public static async Task<AssetPairTradeVolumeResponse> GetPeriodAssetPairTradeVolumeAsync(this ITradeVolumesAPI operations, string assetPairId, System.DateTime fromDate, System.DateTime toDate, CancellationToken cancellationToken = default(CancellationToken))
            {
                using (var _result = await operations.GetPeriodAssetPairTradeVolumeWithHttpMessagesAsync(assetPairId, fromDate, toDate, null, cancellationToken).ConfigureAwait(false))
                {
                    return _result.Body;
                }
            }

            /// <summary>
            /// Calculates trade volume of assetId for clientId within specified time
            /// period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetId'>
            /// Asset Id
            /// </param>
            /// <param name='clientId'>
            /// Client Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            public static AssetTradeVolumeResponse GetPeriodClientAssetTradeVolume(this ITradeVolumesAPI operations, string assetId, string clientId, System.DateTime fromDate, System.DateTime toDate)
            {
                return operations.GetPeriodClientAssetTradeVolumeAsync(assetId, clientId, fromDate, toDate).GetAwaiter().GetResult();
            }

            /// <summary>
            /// Calculates trade volume of assetId for clientId within specified time
            /// period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetId'>
            /// Asset Id
            /// </param>
            /// <param name='clientId'>
            /// Client Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            /// <param name='cancellationToken'>
            /// The cancellation token.
            /// </param>
            public static async Task<AssetTradeVolumeResponse> GetPeriodClientAssetTradeVolumeAsync(this ITradeVolumesAPI operations, string assetId, string clientId, System.DateTime fromDate, System.DateTime toDate, CancellationToken cancellationToken = default(CancellationToken))
            {
                using (var _result = await operations.GetPeriodClientAssetTradeVolumeWithHttpMessagesAsync(assetId, clientId, fromDate, toDate, null, cancellationToken).ConfigureAwait(false))
                {
                    return _result.Body;
                }
            }

            /// <summary>
            /// Calculates trade volume of assetPairId for clientId within specified time
            /// period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetPairId'>
            /// AssetPair Id
            /// </param>
            /// <param name='clientId'>
            /// Client Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            public static AssetPairTradeVolumeResponse GetPeriodClientAssetPairTradeVolume(this ITradeVolumesAPI operations, string assetPairId, string clientId, System.DateTime fromDate, System.DateTime toDate)
            {
                return operations.GetPeriodClientAssetPairTradeVolumeAsync(assetPairId, clientId, fromDate, toDate).GetAwaiter().GetResult();
            }

            /// <summary>
            /// Calculates trade volume of assetPairId for clientId within specified time
            /// period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetPairId'>
            /// AssetPair Id
            /// </param>
            /// <param name='clientId'>
            /// Client Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            /// <param name='cancellationToken'>
            /// The cancellation token.
            /// </param>
            public static async Task<AssetPairTradeVolumeResponse> GetPeriodClientAssetPairTradeVolumeAsync(this ITradeVolumesAPI operations, string assetPairId, string clientId, System.DateTime fromDate, System.DateTime toDate, CancellationToken cancellationToken = default(CancellationToken))
            {
                using (var _result = await operations.GetPeriodClientAssetPairTradeVolumeWithHttpMessagesAsync(assetPairId, clientId, fromDate, toDate, null, cancellationToken).ConfigureAwait(false))
                {
                    return _result.Body;
                }
            }

            /// <summary>
            /// Calculates trade volume of assetId for walletId within specified time
            /// period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetId'>
            /// Asset Id
            /// </param>
            /// <param name='walletId'>
            /// Wallet Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            public static AssetTradeVolumeResponse GetPeriodWalletAssetTradeVolume(this ITradeVolumesAPI operations, string assetId, string walletId, System.DateTime fromDate, System.DateTime toDate)
            {
                return operations.GetPeriodWalletAssetTradeVolumeAsync(assetId, walletId, fromDate, toDate).GetAwaiter().GetResult();
            }

            /// <summary>
            /// Calculates trade volume of assetId for walletId within specified time
            /// period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetId'>
            /// Asset Id
            /// </param>
            /// <param name='walletId'>
            /// Wallet Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            /// <param name='cancellationToken'>
            /// The cancellation token.
            /// </param>
            public static async Task<AssetTradeVolumeResponse> GetPeriodWalletAssetTradeVolumeAsync(this ITradeVolumesAPI operations, string assetId, string walletId, System.DateTime fromDate, System.DateTime toDate, CancellationToken cancellationToken = default(CancellationToken))
            {
                using (var _result = await operations.GetPeriodWalletAssetTradeVolumeWithHttpMessagesAsync(assetId, walletId, fromDate, toDate, null, cancellationToken).ConfigureAwait(false))
                {
                    return _result.Body;
                }
            }

            /// <summary>
            /// Calculates trade volume of assetPairId for walletId within specified time
            /// period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetPairId'>
            /// AssetPair Id
            /// </param>
            /// <param name='walletId'>
            /// Wallet Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            public static AssetPairTradeVolumeResponse GetPeriodWalletAssetPairTradeVolume(this ITradeVolumesAPI operations, string assetPairId, string walletId, System.DateTime fromDate, System.DateTime toDate)
            {
                return operations.GetPeriodWalletAssetPairTradeVolumeAsync(assetPairId, walletId, fromDate, toDate).GetAwaiter().GetResult();
            }

            /// <summary>
            /// Calculates trade volume of assetPairId for walletId within specified time
            /// period.
            /// </summary>
            /// <param name='operations'>
            /// The operations group for this extension method.
            /// </param>
            /// <param name='assetPairId'>
            /// AssetPair Id
            /// </param>
            /// <param name='walletId'>
            /// Wallet Id
            /// </param>
            /// <param name='fromDate'>
            /// Start DateTime (Inclusive) with hour precision
            /// </param>
            /// <param name='toDate'>
            /// Finish DateTime (Exclusive) with hour precision
            /// </param>
            /// <param name='cancellationToken'>
            /// The cancellation token.
            /// </param>
            public static async Task<AssetPairTradeVolumeResponse> GetPeriodWalletAssetPairTradeVolumeAsync(this ITradeVolumesAPI operations, string assetPairId, string walletId, System.DateTime fromDate, System.DateTime toDate, CancellationToken cancellationToken = default(CancellationToken))
            {
                using (var _result = await operations.GetPeriodWalletAssetPairTradeVolumeWithHttpMessagesAsync(assetPairId, walletId, fromDate, toDate, null, cancellationToken).ConfigureAwait(false))
                {
                    return _result.Body;
                }
            }

    }
}
