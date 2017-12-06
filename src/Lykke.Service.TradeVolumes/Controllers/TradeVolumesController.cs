using System;
using System.Net;
using System.Threading.Tasks;
using System.Globalization;
using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.SwaggerGen;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Models;

namespace Lykke.Service.TradeVolumes.Controllers
{
    [Route("api/[controller]")]
    public class TradeVolumesController : Controller
    {
        private const string _dateFormat = "yyyyMMdd";
        private readonly ITradeVolumesCalculator _tradeVolumesCalculator;

        public TradeVolumesController(ITradeVolumesCalculator tradeVolumesCalculator)
        {
            _tradeVolumesCalculator = tradeVolumesCalculator;
        }

        /// <summary>
        /// Calculates trade volume of assetId for clientId on a particular date.
        /// </summary>
        [HttpGet("asset/{assetId}/{clientId}/{dateStr}")]
        [SwaggerOperation("GetAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetAssetTradeVolume(string clientId, string assetId, string dateStr)
        {
            if (string.IsNullOrWhiteSpace(assetId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetId parameter is empty"));

            if (!DateTime.TryParseExact(
                dateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime date))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"DateStr parameter is mulformed - {_dateFormat} format is expected"));

            double tradeVolume = await _tradeVolumesCalculator.GetPeriodAssetVolumeAsync(
                assetId,
                clientId,
                date,
                date);

            return Ok(new AssetTradeVolumeResponse
            {
                AssetId = assetId,
                ClientId = clientId,
                Volume = tradeVolume,
            });
        }

        /// <summary>
        /// Calculates trade volume of assetId for clientId within specified time period.
        /// </summary>
        [HttpGet("asset/{assetId}/{clientId}/{fromDateStr}/{toDateStr}")]
        [SwaggerOperation("GetPeriodAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetPeriodAssetTradeVolume(string clientId, string assetId, string fromDateStr, string toDateStr)
        {
            if (string.IsNullOrWhiteSpace(assetId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetId parameter is empty"));

            if (!DateTime.TryParseExact(
                fromDateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime fromDate))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"DateStr parameter is mulformed - {_dateFormat} format is expected"));

            if (!DateTime.TryParseExact(
                toDateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime toDate))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"DateStr parameter is mulformed - {_dateFormat} format is expected"));

            if (fromDate > toDate)
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"FromDateStr must be earlier than toDateStr"));

            double tradeVolume = await _tradeVolumesCalculator.GetPeriodAssetVolumeAsync(
                assetId,
                clientId,
                fromDate,
                toDate);

            return Ok(new AssetTradeVolumeResponse
            {
                AssetId = assetId,
                ClientId = clientId,
                Volume = tradeVolume,
            });
        }

        /// <summary>
        /// Calculates trade volume of assetId for clientId on a particular date.
        /// </summary>
        [HttpGet("pair/{assetId}/{clientId}/{dateStr}")]
        [SwaggerOperation("GetAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetPairTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetAssetPairTradeVolume(string clientId, string assetPairId, string dateStr)
        {
            if (string.IsNullOrWhiteSpace(assetPairId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetPairId parameter is empty"));

            if (!DateTime.TryParseExact(
                dateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime date))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"DateStr parameter is mulformed - {_dateFormat} format is expected"));

            (double baseVolume, double quotingVolume) = await _tradeVolumesCalculator.GetPeriodAssetPairVolumeAsync(
                assetPairId,
                clientId,
                date,
                date);

            return Ok(new AssetPairTradeVolumeResponse
            {
                AssetPairId = assetPairId,
                ClientId = clientId,
                BaseVolume = baseVolume,
                QuotingVolume = quotingVolume,
            });
        }

        /// <summary>
        /// Calculates trade volume of assetId for clientId within specified time period.
        /// </summary>
        [HttpGet("pair/{assetId}/{clientId}/{fromDateStr}/{toDateStr}")]
        [SwaggerOperation("GetPeriodAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetPairTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetPeriodAssetPairTradeVolume(string clientId, string assetPairId, string fromDateStr, string toDateStr)
        {
            if (string.IsNullOrWhiteSpace(assetPairId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetPairId parameter is empty"));

            if (!DateTime.TryParseExact(
                fromDateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime fromDate))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"DateStr parameter is mulformed - {_dateFormat} format is expected"));

            if (!DateTime.TryParseExact(
                toDateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime toDate))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"DateStr parameter is mulformed - {_dateFormat} format is expected"));

            if (fromDate > toDate)
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"FromDateStr must be earlier than toDateStr"));

            (double baseVolume, double quotingVolume) = await _tradeVolumesCalculator.GetPeriodAssetPairVolumeAsync(
                assetPairId,
                clientId,
                fromDate,
                toDate);

            return Ok(new AssetPairTradeVolumeResponse
            {
                AssetPairId = assetPairId,
                ClientId = clientId,
                BaseVolume = baseVolume,
                QuotingVolume = quotingVolume,
            });
        }
    }
}
