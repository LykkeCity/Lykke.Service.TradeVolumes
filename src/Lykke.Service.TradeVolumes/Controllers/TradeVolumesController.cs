using System;
using System.Net;
using System.Threading.Tasks;
using System.Globalization;
using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.SwaggerGen;
using Lykke.Service.TradeVolumes.Core;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Services;
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
        /// Calculates trade volume of assetId on a particular date.
        /// </summary>
        /// <param name="assetId">Asset Id</param>
        /// <param name="dateStr">Date in yyyyMMdd string format</param>
        [HttpGet("asset/{assetId}/all/{dateStr}")]
        [SwaggerOperation("GetAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetAssetTradeVolume(string assetId, string dateStr)
        {
            return await GetClientAssetTradeVolume(
                assetId,
                TradeVolumesCalculator.AllClients,
                dateStr);
        }

        /// <summary>
        /// Calculates trade volume of assetId within specified time period.
        /// </summary>
        /// <param name="assetId">Asset Id</param>
        /// <param name="fromDateStr">Start date in yyyyMMdd string format (Inclusive)</param>
        /// <param name="toDateStr">Finish date in yyyyMMdd string format (Exclusive)</param>
        [HttpGet("asset/{assetId}/all/{fromDateStr}/{toDateStr}")]
        [SwaggerOperation("GetPeriodClientAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetPeriodAssetTradeVolume(
            string assetId,
            string fromDateStr,
            string toDateStr)
        {
            return await GetPeriodClientAssetTradeVolume(
                assetId,
                TradeVolumesCalculator.AllClients,
                fromDateStr,
                toDateStr);
        }

        /// <summary>
        /// Calculates trade volume of assetPairId on a particular date.
        /// </summary>
        /// <param name="assetPairId">AssetPair Id</param>
        /// <param name="dateStr">Date in yyyyMMdd string format</param>
        [HttpGet("pair/{assetPairId}/all/{dateStr}")]
        [SwaggerOperation("GetClientAssetPairTradeVolume")]
        [ProducesResponseType(typeof(AssetPairTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetAssetPairTradeVolume(string assetPairId, string dateStr)
        {
            return await GetClientAssetPairTradeVolume(
                assetPairId,
                TradeVolumesCalculator.AllClients,
                dateStr);
        }

        /// <summary>
        /// Calculates trade volume of assetPairId within specified time period.
        /// </summary>
        /// <param name="assetPairId">AssetPair Id</param>
        /// <param name="fromDateStr">Start date in yyyyMMdd string format (Inclusive)</param>
        /// <param name="toDateStr">Finish date in yyyyMMdd string format (Exclusive)</param>
        [HttpGet("pair/{assetPairId}/all/{fromDateStr}/{toDateStr}")]
        [SwaggerOperation("GetPeriodClientAssetPairTradeVolume")]
        [ProducesResponseType(typeof(AssetPairTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetPeriodAssetPairTradeVolume(
            string assetPairId,
            string fromDateStr,
            string toDateStr)
        {
            return await GetPeriodClientAssetPairTradeVolume(
                assetPairId,
                TradeVolumesCalculator.AllClients,
                fromDateStr,
                toDateStr);
        }

        /// <summary>
        /// Calculates trade volume of assetId for clientId on a particular date.
        /// </summary>
        /// <param name="assetId">Asset Id</param>
        /// <param name="clientId">Client Id</param>
        /// <param name="dateStr">Date in yyyyMMdd string format</param>
        [HttpGet("asset/{assetId}/{clientId}/{dateStr}")]
        [SwaggerOperation("GetClientAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetClientAssetTradeVolume(
            string assetId,
            string clientId,
            string dateStr)
        {
            if (string.IsNullOrWhiteSpace(assetId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetId parameter is empty"));

            if (string.IsNullOrWhiteSpace(clientId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("ClientId parameter is empty"));

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

            return Ok(
                new AssetTradeVolumeResponse
                {
                    AssetId = assetId,
                    ClientId = clientId,
                    Volume = tradeVolume,
                });
        }

        /// <summary>
        /// Calculates trade volume of assetId for clientId within specified time period.
        /// </summary>
        /// <param name="assetId">Asset Id</param>
        /// <param name="clientId">Client Id</param>
        /// <param name="fromDateStr">Start date in yyyyMMdd string format (Inclusive)</param>
        /// <param name="toDateStr">Finish date in yyyyMMdd string format (Exclusive)</param>
        [HttpGet("asset/{assetId}/{clientId}/{fromDateStr}/{toDateStr}")]
        [SwaggerOperation("GetPeriodClientAssetTradeVolume")]
        [ProducesResponseType(typeof(AssetTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetPeriodClientAssetTradeVolume(
            string assetId,
            string clientId,
            string fromDateStr,
            string toDateStr)
        {
            if (string.IsNullOrWhiteSpace(assetId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetId parameter is empty"));

            if (string.IsNullOrWhiteSpace(clientId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("ClientId parameter is empty"));

            if (!DateTime.TryParseExact(
                fromDateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime fromDate))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"FromDateStr parameter is mulformed - {_dateFormat} format is expected"));

            if (!DateTime.TryParseExact(
                toDateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime toDate))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"ToDateStr parameter is mulformed - {_dateFormat} format is expected"));

            if (fromDate > toDate)
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"FromDateStr must be earlier than toDateStr"));

            double tradeVolume = await _tradeVolumesCalculator.GetPeriodAssetVolumeAsync(
                assetId,
                clientId,
                fromDate,
                toDate);

            return Ok(
                new AssetTradeVolumeResponse
                {
                    AssetId = assetId,
                    ClientId = clientId,
                    Volume = tradeVolume,
                });
        }

        /// <summary>
        /// Calculates trade volume of assetPairId for clientId on a particular date.
        /// </summary>
        /// <param name="assetPairId">AssetPair Id</param>
        /// <param name="clientId">Client Id</param>
        /// <param name="dateStr">Date in yyyyMMdd string format</param>
        [HttpGet("pair/{assetPairId}/{clientId}/{dateStr}")]
        [SwaggerOperation("GetClientAssetPairTradeVolume")]
        [ProducesResponseType(typeof(AssetPairTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetClientAssetPairTradeVolume(
            string assetPairId,
            string clientId,
            string dateStr)
        {
            if (string.IsNullOrWhiteSpace(assetPairId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetPairId parameter is empty"));

            if (string.IsNullOrWhiteSpace(clientId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("ClientId parameter is empty"));

            if (!DateTime.TryParseExact(
                dateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime date))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"DateStr parameter is mulformed - {_dateFormat} format is expected"));

            try
            {
                (double baseVolume, double quotingVolume) = await _tradeVolumesCalculator.GetPeriodAssetPairVolumeAsync(
                    assetPairId,
                    clientId,
                    date,
                    date);

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
        /// Calculates trade volume of assetPairId for clientId within specified time period.
        /// </summary>
        /// <param name="assetPairId">AssetPair Id</param>
        /// <param name="clientId">Client Id</param>
        /// <param name="fromDateStr">Start date in yyyyMMdd string format (Inclusive)</param>
        /// <param name="toDateStr">Finish date in yyyyMMdd string format (Exclusive)</param>
        [HttpGet("pair/{assetPairId}/{clientId}/{fromDateStr}/{toDateStr}")]
        [SwaggerOperation("GetPeriodClientAssetPairTradeVolume")]
        [ProducesResponseType(typeof(AssetPairTradeVolumeResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.BadRequest)]
        public async Task<IActionResult> GetPeriodClientAssetPairTradeVolume(
            string assetPairId,
            string clientId,
            string fromDateStr,
            string toDateStr)
        {
            if (string.IsNullOrWhiteSpace(assetPairId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("AssetPairId parameter is empty"));

            if (string.IsNullOrWhiteSpace(clientId))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create("ClientId parameter is empty"));

            if (!DateTime.TryParseExact(
                fromDateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime fromDate))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"FromDateStr parameter is mulformed - {_dateFormat} format is expected"));

            if (!DateTime.TryParseExact(
                toDateStr,
                _dateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal,
                out DateTime toDate))
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"ToDateStr parameter is mulformed - {_dateFormat} format is expected"));

            if (fromDate > toDate)
                return StatusCode(
                    (int)HttpStatusCode.BadRequest,
                    ErrorResponse.Create($"FromDateStr must be earlier than toDateStr"));

            try
            {
                (double baseVolume, double quotingVolume) = await _tradeVolumesCalculator.GetPeriodAssetPairVolumeAsync(
                    assetPairId,
                    clientId,
                    fromDate,
                    toDate);

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
    }
}
