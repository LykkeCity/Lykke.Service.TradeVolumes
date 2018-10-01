using Lykke.SettingsReader.Attributes;

namespace Lykke.Service.TradeVolumes.Settings
{
    public class AppSettings
    {
        public TradeVolumesSettings TradeVolumesService { get; set; }

        public SlackNotificationsSettings SlackNotifications { get; set; }

        public AssetsServiceClientSettings AssetsServiceClient { get; set; }

        public MonitoringServiceClientSettings MonitoringServiceClient { get; set; }
    }

    public class AssetsServiceClientSettings
    {
        [HttpCheck("api/isalive")]
        public string ServiceUrl { get; set; }
    }

    public class SlackNotificationsSettings
    {
        public AzureQueuePublicationSettings AzureQueue { get; set; }
    }

    public class AzureQueuePublicationSettings
    {
        public string ConnectionString { get; set; }

        public string QueueName { get; set; }
    }

    public class MonitoringServiceClientSettings
    {
        [HttpCheck("api/isalive")]
        public string MonitoringServiceUrl { get; set; }
    }

    public class TradeVolumesSettings
    {
        [AzureTableCheck]
        public string LogsConnString { get; set; }

        [AzureTableCheck]
        public string TradeVolumesConnString { get; set; }

        public string RedisConnString { get; set; }
    }
}
