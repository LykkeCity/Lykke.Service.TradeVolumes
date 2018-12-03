using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Common.Log;
using Lykke.RabbitMqBroker;
using Lykke.RabbitMqBroker.Subscriber;
using Lykke.Job.TradesConverter.Contract;
using Lykke.Service.TradeVolumes.Core.Services;

namespace Lykke.Job.TradeVolumes.Subscribers
{
    internal class TradelogSubscriber : IStartStop
    {
        private readonly ILogFactory _logFactory;
        private readonly ILog _log;
        private readonly string _connectionString;
        private readonly string _exchangeName;
        private readonly ITradeVolumesCalculator _tradeVolumesCalculator;

        private RabbitMqSubscriber<List<TradeLogItem>> _subscriber;

        public TradelogSubscriber(
            ITradeVolumesCalculator tradeVolumesCalculator,
            ILogFactory logFactory,
            string connectionString,
            string exchangeName)
        {
            _tradeVolumesCalculator = tradeVolumesCalculator;
            _logFactory = logFactory;
            _log = logFactory.CreateLog(this);
            _connectionString = connectionString;
            _exchangeName = exchangeName;
        }

        public void Start()
        {
            var settings = RabbitMqSubscriptionSettings
                .ForSubscriber(_connectionString, _exchangeName, "tradevolumes")
                .MakeDurable();

            _subscriber = new RabbitMqSubscriber<List<TradeLogItem>>(
                    _logFactory,
                    settings,
                    new ResilientErrorHandlingStrategy(
                        _logFactory,
                        settings,
                        retryTimeout: TimeSpan.FromSeconds(10),
                        next: new DeadQueueErrorHandlingStrategy(_logFactory, settings)))
                .SetMessageDeserializer(new MessagePackMessageDeserializer<List<TradeLogItem>>())
                .SetMessageReadStrategy(new MessageReadQueueStrategy())
                .Subscribe(ProcessMessageAsync)
                .CreateDefaultBinding()
                .Start();
        }

        private async Task ProcessMessageAsync(List<TradeLogItem> arg)
        {
            try
            {
                await _tradeVolumesCalculator.AddTradeLogItemsAsync(arg);
            }
            catch (Exception ex)
            {
                _log.Error(ex, context: arg);
                throw;
            }
        }

        public void Dispose()
        {
            _subscriber?.Dispose();
        }

        public void Stop()
        {
            _subscriber?.Stop();
        }
    }
}
