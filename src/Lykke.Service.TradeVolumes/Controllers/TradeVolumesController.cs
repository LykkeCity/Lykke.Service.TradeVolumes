using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.SwaggerGen;
using Common;
using Lykke.Service.TradeVolumes.Core;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Services;
using Lykke.Service.TradeVolumes.Models;

namespace Lykke.Service.TradeVolumes.Controllers
{
    [Route("api/[controller]")]
    public class TradeVolumesController : Controller
    {
        private readonly ITradeVolumesCalculator _tradeVolumesCalculator;

        public TradeVolumesController(ITradeVolumesCalculator tradeVolumesCalculator)
        {
            _tradeVolumesCalculator = tradeVolumesCalculator;
        }

        /// <summary>
        /// Calculates trade volume of assetId within specified time period.
        /// </summary>
        /// <param name="assetId">Asset Id</param>
        /// <param name="fromDate">Start DateTime (Inclusive) with hour precision</param>
        /// <param name="toDate">Finish DateTime (Exclusive) with hour precision</param>
        [HttpGet("asset/{assetId}/all/{fromDate}/{toDate}")]
        [SwaggerOperation("GetPeriodAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetTradeVolumeResponse), (int)HttpStatusCode.OK)]
        public Task<IActionResult> GetPeriodAssetTradeVolume(
            string assetId,
            DateTime fromDate,
            DateTime toDate)
        {
            return GetPeriodClientAssetTradeVolume(
                assetId,
                Constants.AllClients,
                fromDate,
                toDate);
        }

        /// <summary>
        /// Calculates trade volume of assetPairId within specified time period.
        /// </summary>
        /// <param name="assetPairId">AssetPair Id</param>
        /// <param name="fromDate">Start DateTime (Inclusive) with hour precision</param>
        /// <param name="toDate">Finish DateTime (Exclusive) with hour precision</param>
        [HttpGet("pair/{assetPairId}/all/{fromDate}/{toDate}")]
        [SwaggerOperation("GetPeriodAssetPairTradeVolume")]
        [ProducesResponseType(typeof(AssetPairTradeVolumeResponse), (int)HttpStatusCode.OK)]
        public Task<IActionResult> GetPeriodAssetPairTradeVolume(
            string assetPairId,
            DateTime fromDate,
            DateTime toDate)
        {
            return GetPeriodClientAssetPairTradeVolume(
                assetPairId,
                Constants.AllClients,
                fromDate,
                toDate);
        }

        /// <summary>
        /// Calculates trade volume of assetId for clientId within specified time period.
        /// </summary>
        /// <param name="assetId">Asset Id</param>
        /// <param name="clientId">Client Id</param>
        /// <param name="fromDate">Start DateTime (Inclusive) with hour precision</param>
        /// <param name="toDate">Finish DateTime (Exclusive) with hour precision</param>
        [HttpGet("asset/{assetId}/client/{clientId}/{fromDate}/{toDate}")]
        [SwaggerOperation("GetPeriodClientAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetTradeVolumeResponse), (int)HttpStatusCode.OK)]
        public async Task<IActionResult> GetPeriodClientAssetTradeVolume(
            string assetId,
            string clientId,
            DateTime fromDate,
            DateTime toDate)
        {
            if (string.IsNullOrWhiteSpace(assetId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetId parameter is empty"));

            if (string.IsNullOrWhiteSpace(clientId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("ClientId parameter is empty"));

            if (fromDate >= toDate)
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"fromDate must be earlier than toDate"));

            double tradeVolume = await GetPeriodAssetTradeVolume(
                assetId,
                clientId,
                fromDate,
                toDate,
                true);

            return Ok(
                new AssetTradeVolumeResponse
                {
                    AssetId = assetId,
                    ClientId = clientId,
                    Volume = tradeVolume,
                });
        }

        /// <summary>
        /// Calculates trade volume of assetPairId for clientId within specified time period.
        /// </summary>
        /// <param name="assetPairId">AssetPair Id</param>
        /// <param name="clientId">Client Id</param>
        /// <param name="fromDate">Start DateTime (Inclusive) with hour precision</param>
        /// <param name="toDate">Finish DateTime (Exclusive) with hour precision</param>
        [HttpGet("pair/{assetPairId}/client/{clientId}/{fromDate}/{toDate}")]
        [SwaggerOperation("GetPeriodClientAssetPairTradeVolume")]
        [ProducesResponseType(typeof(AssetPairTradeVolumeResponse), (int)HttpStatusCode.OK)]
        public async Task<IActionResult> GetPeriodClientAssetPairTradeVolume(
            string assetPairId,
            string clientId,
            DateTime fromDate,
            DateTime toDate)
        {
            if (string.IsNullOrWhiteSpace(assetPairId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetPairId parameter is empty"));

            if (string.IsNullOrWhiteSpace(clientId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("ClientId parameter is empty"));

            if (fromDate >= toDate)
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"fromDate must be earlier than toDate"));

            try
            {
                (double baseVolume, double quotingVolume) = await GetPeriodAssetPairTradeVolume(
                    assetPairId,
                    clientId,
                    fromDate,
                    toDate,
                    true);

                return Ok(
                    new AssetPairTradeVolumeResponse
                    {
                        AssetPairId = assetPairId,
                        ClientId = clientId,
                        BaseVolume = baseVolume,
                        QuotingVolume = quotingVolume,
                    });
            }
            catch (UnknownPairException ex)
            {
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create(ex.Message));
            }
        }

        /// <summary>
        /// Calculates trade volume of assetId for walletId within specified time period.
        /// </summary>
        /// <param name="assetId">Asset Id</param>
        /// <param name="walletId">Wallet Id</param>
        /// <param name="fromDate">Start DateTime (Inclusive) with hour precision</param>
        /// <param name="toDate">Finish DateTime (Exclusive) with hour precision</param>
        [HttpGet("asset/{assetId}/wallet/{walletId}/{fromDate}/{toDate}")]
        [SwaggerOperation("GetPeriodWalletAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetTradeVolumeResponse), (int)HttpStatusCode.OK)]
        public async Task<IActionResult> GetPeriodWalletAssetTradeVolume(
            string assetId,
            string walletId,
            DateTime fromDate,
            DateTime toDate)
        {
            if (string.IsNullOrWhiteSpace(assetId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetId parameter is empty"));

            if (string.IsNullOrWhiteSpace(walletId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("WalletId parameter is empty"));

            if (fromDate >= toDate)
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"fromDate must be earlier than toDate"));

            double tradeVolume = await GetPeriodAssetTradeVolume(
                assetId,
                walletId,
                fromDate,
                toDate,
                false);

            return Ok(
                new AssetTradeVolumeResponse
                {
                    AssetId = assetId,
                    WalletId = walletId,
                    Volume = tradeVolume,
                });
        }

        /// <summary>
        /// Calculates trade volume of assetPairId for walletId within specified time period.
        /// </summary>
        /// <param name="assetPairId">AssetPair Id</param>
        /// <param name="walletId">Wallet Id</param>
        /// <param name="fromDate">Start DateTime (Inclusive) with hour precision</param>
        /// <param name="toDate">Finish DateTime (Exclusive) with hour precision</param>
        [HttpGet("pair/{assetPairId}/wallet/{walletId}/{fromDate}/{toDate}")]
        [SwaggerOperation("GetPeriodWalletAssetPairTradeVolume")]
        [ProducesResponseType(typeof(AssetPairTradeVolumeResponse), (int)HttpStatusCode.OK)]
        public async Task<IActionResult> GetPeriodWalletAssetPairTradeVolume(
            string assetPairId,
            string walletId,
            DateTime fromDate,
            DateTime toDate)
        {
            if (string.IsNullOrWhiteSpace(assetPairId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetPairId parameter is empty"));

            if (string.IsNullOrWhiteSpace(walletId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("WalletId parameter is empty"));

            if (fromDate >= toDate)
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"fromDate must be earlier than toDate"));

            try
            {
                (double baseVolume, double quotingVolume) = await GetPeriodAssetPairTradeVolume(
                    assetPairId,
                    walletId,
                    fromDate,
                    toDate,
                    false);

                return Ok(
                    new AssetPairTradeVolumeResponse
                    {
                        AssetPairId = assetPairId,
                        WalletId = walletId,
                        BaseVolume = baseVolume,
                        QuotingVolume = quotingVolume,
                    });
            }
            catch (UnknownPairException ex)
            {
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create(ex.Message));
            }
        }

        private async Task<double> GetPeriodAssetTradeVolume(
            string assetId,
            string id,
            DateTime fromDate,
            DateTime toDate,
            bool isUser)
        {
            if (fromDate.Kind == DateTimeKind.Unspecified)
                fromDate = DateTime.SpecifyKind(fromDate, DateTimeKind.Utc);
            else if (fromDate.Kind == DateTimeKind.Local)
                fromDate = fromDate.ToUniversalTime();
            fromDate = fromDate.RoundToHour();
            if (toDate.Kind == DateTimeKind.Unspecified)
                toDate = DateTime.SpecifyKind(toDate, DateTimeKind.Utc);
            else if (toDate.Kind == DateTimeKind.Local)
                toDate = toDate.ToUniversalTime();
            toDate = toDate.RoundToHour();

            if (isUser)
                id = ClientIdHashHelper.GetClientIdHash(id);

            var result = await _tradeVolumesCalculator.GetPeriodAssetVolumeAsync(
                assetId,
                id,
                fromDate,
                toDate,
                isUser);

            // For trading wallets hashed ClientId instead of WalletId is stored as WalletId
            if (!isUser && result == 0)
            {
                result = await _tradeVolumesCalculator.GetPeriodAssetVolumeAsync(
                    assetId,
                    ClientIdHashHelper.GetClientIdHash(id),
                    fromDate,
                    toDate,
                    isUser);
            }

            return result;
        }

        private async Task<(double, double)> GetPeriodAssetPairTradeVolume(
            string assetPairId,
            string id,
            DateTime fromDate,
            DateTime toDate,
            bool isUser)
        {
            if (fromDate.Kind == DateTimeKind.Unspecified)
                fromDate = DateTime.SpecifyKind(fromDate, DateTimeKind.Utc);
            else if (fromDate.Kind == DateTimeKind.Local)
                fromDate = fromDate.ToUniversalTime();
            fromDate = fromDate.RoundToHour();
            if (toDate.Kind == DateTimeKind.Unspecified)
                toDate = DateTime.SpecifyKind(toDate, DateTimeKind.Utc);
            else if (toDate.Kind == DateTimeKind.Local)
                toDate = toDate.ToUniversalTime();
            toDate = toDate.RoundToHour();

            if (isUser)
                id = ClientIdHashHelper.GetClientIdHash(id);

            var result = await _tradeVolumesCalculator.GetPeriodAssetPairVolumeAsync(
                assetPairId,
                id,
                fromDate,
                toDate,
                isUser);

            // For trading wallets hashed ClientId instead of WalletId is stored as WalletId
            if (!isUser && result.Item1 == 0 && result.Item2 == 0)
            {
                result = await _tradeVolumesCalculator.GetPeriodAssetPairVolumeAsync(
                    assetPairId,
                    ClientIdHashHelper.GetClientIdHash(id),
                    fromDate,
                    toDate,
                    isUser);
            }

            return result;
        }
    }
}
