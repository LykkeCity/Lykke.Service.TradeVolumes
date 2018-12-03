using System;
using Autofac;
using Lykke.Sdk;
using Lykke.Sdk.Health;
using Lykke.SettingsReader;
using Lykke.Service.Assets.Client;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Core.Repositories;
using Lykke.Service.TradeVolumes.Services;
using Lykke.Service.TradeVolumes.AzureRepositories;
using Lykke.Service.TradeVolumes.Settings;
using StackExchange.Redis;

namespace Lykke.Service.TradeVolumes.Modules
{
    public class ServiceModule : Module
    {
        private readonly IReloadingManager<AppSettings> _settingsManager;
        private readonly AppSettings _settings;

        public ServiceModule(IReloadingManager<AppSettings> settingsManager)
        {
            _settingsManager = settingsManager;
            _settings = settingsManager.CurrentValue;
        }

        protected override void Load(ContainerBuilder builder)
        {
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

            builder.RegisterType<TradeVolumesCalculator>()
                .As<ITradeVolumesCalculator>()
                .SingleInstance()
                .WithParameter("warningDelay", null);
        }
    }
}
