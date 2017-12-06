namespace Lykke.Service.TradeVolumes.Core.Settings
{
    public class AppSettings
    {
        public TradeVolumesSettings TradeVolumesService { get; set; }

        public SlackNotificationsSettings SlackNotifications { get; set; }
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

    public class TradeVolumesSettings
    {
        public string LogsConnString { get; set; }

        public string RabbitMqConnString { get; set; }

        public string TradesExchangeName { get; set; }
    }
}
