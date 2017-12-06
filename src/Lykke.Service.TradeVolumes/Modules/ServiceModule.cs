using System;
using Autofac;
using Common;
using Common.Log;
using Lykke.SettingsReader;
using Lykke.Service.Assets.Client;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Core.Repositories;
using Lykke.Service.TradeVolumes.Services;
using Lykke.Service.TradeVolumes.AzureRepositories;
using Lykke.Service.TradeVolumes.Settings;
using Lykke.Service.TradeVolumes.Subscribers;

namespace Lykke.Service.TradeVolumes.Modules
{
    public class ServiceModule : Module
    {
        private readonly IReloadingManager<AppSettings> _settingsManager;
        private readonly AppSettings _settings;
        private readonly IConsole _console;
        private readonly ILog _log;

        public ServiceModule(
            IReloadingManager<AppSettings> settingsManager,
            IConsole console,
            ILog log)
        {
            _settingsManager = settingsManager;
            _settings = settingsManager.CurrentValue;
            _console = console;
            _log = log;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterInstance(_console)
                .As<IConsole>()
                .SingleInstance();

            builder.RegisterInstance(_log)
                .As<ILog>()
                .SingleInstance();

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>();

            var tradeVolumesRepository = new TradeVolumesRepository(_settingsManager.Nested(x => x.TradeVolumesService.TradeVolumesConnString), _log);
            builder.RegisterInstance(tradeVolumesRepository)
                .As<ITradeVolumesRepository>()
                .SingleInstance();

            var assetsService = new AssetsService(new Uri(_settings.AssetsServiceClient.ServiceUrl));
            builder.RegisterInstance(assetsService)
                .As<IAssetsService>()
                .SingleInstance();

            builder.RegisterType<TradeVolumesCalculator>()
                .As<ITradeVolumesCalculator>()
                .SingleInstance();

            builder.RegisterType<TradelogSubscriber>()
                .As<IStartable>()
                .As<IStopable>()
                .AutoActivate()
                .SingleInstance()
                .WithParameter("connectionString", _settings.TradeVolumesService.RabbitMqConnString)
                .WithParameter("exchangeName", _settings.TradeVolumesService.TradelogExchangeName);
        }
    }
}
