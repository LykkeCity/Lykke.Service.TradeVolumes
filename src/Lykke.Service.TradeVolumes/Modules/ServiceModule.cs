using System;
using Autofac;
using Common.Log;
using Lykke.Common;
using Lykke.SettingsReader;
using Lykke.Service.Assets.Client;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Core.Repositories;
using Lykke.Service.TradeVolumes.Services;
using Lykke.Service.TradeVolumes.AzureRepositories;
using Lykke.Service.TradeVolumes.Settings;
using Lykke.Service.TradeVolumes.Subscribers;
using StackExchange.Redis;

namespace Lykke.Service.TradeVolumes.Modules
{
    public class ServiceModule : Module
    {
        private readonly IReloadingManager<AppSettings> _settingsManager;
        private readonly AppSettings _settings;
        private readonly ILog _log;

        public ServiceModule(IReloadingManager<AppSettings> settingsManager, ILog log)
        {
            _settingsManager = settingsManager;
            _settings = settingsManager.CurrentValue;
            _log = log;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterInstance(_log)
                .As<ILog>()
                .SingleInstance();

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>()
                .SingleInstance();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>()
                .AutoActivate()
                .SingleInstance();

            builder.RegisterResourcesMonitoring(_log);

            builder.Register(context => ConnectionMultiplexer.Connect(_settings.TradeVolumesService.RedisConnString))
                .As<IConnectionMultiplexer>()
                .SingleInstance();

            builder.RegisterType<AssetsService>()
                .WithParameter(TypedParameter.From(new Uri(_settings.AssetsServiceClient.ServiceUrl)))
                .As<IAssetsService>()
                .SingleInstance();

            builder.RegisterType<CachesManager>()
                .As<ICachesManager>()
                .SingleInstance();

            builder.RegisterType<AssetsDictionary>()
                .As<IAssetsDictionary>()
                .SingleInstance();

            builder.RegisterType<TradeVolumesRepository>()
                .WithParameter(TypedParameter.From(_settingsManager.Nested(x => x.TradeVolumesService.TradeVolumesConnString)))
                .As<ITradeVolumesRepository>()
                .SingleInstance();

            int warningHoursDelay = 1;
            if (_settings.TradeVolumesService.WarningDelayInHours > 0)
                warningHoursDelay = _settings.TradeVolumesService.WarningDelayInHours;
            int tradesCacheHoursTimeout = 6;
            if (_settings.TradeVolumesService.TradesCacheTimeoutInHours > 0)
                tradesCacheHoursTimeout = _settings.TradeVolumesService.TradesCacheTimeoutInHours;
            builder.RegisterType<TradeVolumesCalculator>()
                .As<ITradeVolumesCalculator>()
                .SingleInstance()
                .WithParameter("warningDelay", TimeSpan.FromHours(warningHoursDelay))
                .WithParameter("cacheTimeout", TimeSpan.FromHours(tradesCacheHoursTimeout));

            if (!_settings.TradeVolumesService.DisableRabbitMqConnection.HasValue || !_settings.TradeVolumesService.DisableRabbitMqConnection.Value)
                builder.RegisterType<TradelogSubscriber>()
                    .As<IStartStop>()
                    .SingleInstance()
                    .WithParameter("connectionString", _settings.TradeVolumesService.RabbitMqConnString)
                    .WithParameter("exchangeName", _settings.TradeVolumesService.TradelogExchangeName);
        }
    }
}
