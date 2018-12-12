using Lykke.Sdk.Settings;
using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.TradeVolumes.Settings
{
    public class AppSettings : BaseAppSettings
    {
        public TradeVolumesJobSettings TradeVolumesJob { get; set; }

        public AssetsServiceClientSettings AssetsServiceClient { get; set; }
    }

    public class AssetsServiceClientSettings
    {
        [HttpCheck("api/isalive")]
        public string ServiceUrl { get; set; }
    }

    public class TradeVolumesJobSettings
    {
        public DbSettings Db { get; set; }

        public RabbitMqSettings Rabbit { get; set; }

        public string RedisConnString { get; set; }
    }

    public class DbSettings
    {
        [AzureTableCheck]
        public string LogsConnString { get; set; }

        [AzureTableCheck]
        public string TradeVolumesConnString { get; set; }
    }

    public class RabbitMqSettings
    {
        [AmqpCheck]
        public string ConnectionString { get; set; }

        public string ExchangeName { get; set; }
    }
}
