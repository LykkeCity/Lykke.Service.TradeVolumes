﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using Lykke.RabbitMqBroker;
using Lykke.RabbitMqBroker.Subscriber;
using Lykke.Job.TradesConverter.Contract;
using Lykke.Service.TradeVolumes.Core.Services;

namespace Lykke.Service.TradeVolumes.Subscribers
{
    internal class TradelogSubscriber : IStartStop
    {
        private readonly ILog _log;
        private readonly string _connectionString;
        private readonly string _exchangeName;
        private readonly ITradeVolumesCalculator _tradeVolumesCalculator;

        private RabbitMqSubscriber<List<TradeLogItem>> _subscriber;

        public TradelogSubscriber(
            ITradeVolumesCalculator tradeVolumesCalculator,
            ILog log,
            string connectionString,
            string exchangeName)
        {
            _tradeVolumesCalculator = tradeVolumesCalculator;
            _log = log;
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
                _log.WriteError("TradelogSubscriber.ProcessMessageAsync", arg, ex);
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
