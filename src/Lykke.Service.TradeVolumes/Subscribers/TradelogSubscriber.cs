using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Autofac;
using Common;
using Common.Log;
using Lykke.RabbitMqBroker;
using Lykke.RabbitMqBroker.Subscriber;
using Lykke.Job.TradesConverter.Contract;
using Lykke.Service.TradeVolumes.Core.Services;

namespace Lykke.Service.TradeVolumes.Subscribers
{
    internal class TradelogSubscriber : IStartable, IStopable
    {
        private readonly ILog _log;
        private readonly IConsole _console;
        private readonly string _connectionString;
        private readonly string _exchangeName;
        private readonly ITradeVolumesCalculator _tradeVolumesCalculator;
        private RabbitMqSubscriber<List<TradeLogItem>> _subscriber;

        public TradelogSubscriber(
            ITradeVolumesCalculator tradeVolumesCalculator,
            ILog log,
            IConsole console,
            string connectionString,
            string exchangeName)
        {
            _tradeVolumesCalculator = tradeVolumesCalculator;
            _log = log;
            _console = console;
            _connectionString = connectionString;
            _exchangeName = exchangeName;
        }

        public void Start()
        {
            var settings = RabbitMqSubscriptionSettings
                .CreateForSubscriber(_connectionString, _exchangeName, "tradevolumes")
                .MakeDurable();

            _subscriber = new RabbitMqSubscriber<List<TradeLogItem>>(settings,
                    new ResilientErrorHandlingStrategy(_log, settings,
                        retryTimeout: TimeSpan.FromSeconds(10),
                        next: new DeadQueueErrorHandlingStrategy(_log, settings)))
                .SetMessageDeserializer(new MessagePackMessageDeserializer<List<TradeLogItem>>())
                .SetMessageReadStrategy(new MessageReadQueueStrategy())
                .Subscribe(ProcessMessageAsync)
                .CreateDefaultBinding()
                .SetLogger(_log)
                .SetConsole(_console)
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
                await _log.WriteErrorAsync("TradelogSubscriber.ProcessMessageAsync", arg.ToJson(), ex);
                throw;
            }
        }

        public void Dispose()
        {
            _subscriber?.Dispose();
        }

        public void Stop()
        {
            _subscriber.Stop();
        }
    }
}
