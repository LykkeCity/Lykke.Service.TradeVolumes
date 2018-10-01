using System;
using Autofac;
using Lykke.Job.TradeVolumes.Settings;
using Lykke.Job.TradeVolumes.Subscribers;
using Lykke.Sdk;
using Lykke.Service.Assets.Client;
using Lykke.Service.TradeVolumes.AzureRepositories;
using Lykke.Service.TradeVolumes.Core.Repositories;
using Lykke.Service.TradeVolumes.Core.Services;
using Lykke.Service.TradeVolumes.Services;
using Lykke.SettingsReader;
using StackExchange.Redis;

namespace Lykke.Job.TradeVolumes.Modules
{
    public class JobModule : Module
    {
        private readonly AppSettings _appSettings;
        private readonly IReloadingManager<TradeVolumesJobSettings> _settingsManager;

        public JobModule(AppSettings appSettings, IReloadingManager<TradeVolumesJobSettings> settingsManager)
        {
            _appSettings = appSettings;
            _settingsManager = settingsManager;
        }

        protected override void Load(ContainerBuilder builder)
        {
            var settings = _appSettings.TradeVolumesJob;

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>()
                .SingleInstance();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>()
                .AutoActivate()
                .SingleInstance();

            builder.Register(context => ConnectionMultiplexer.Connect(settings.RedisConnString))
                .As<IConnectionMultiplexer>()
                .SingleInstance();

            builder.RegisterType<AssetsService>()
                .WithParameter(TypedParameter.From(new Uri(_appSettings.AssetsServiceClient.ServiceUrl)))
                .As<IAssetsService>()
                .SingleInstance();

            builder.RegisterType<CachesManager>()
                .As<ICachesManager>()
                .SingleInstance();

            builder.RegisterType<AssetsDictionary>()
                .As<IAssetsDictionary>()
                .SingleInstance();

            builder.RegisterType<TradeVolumesRepository>()
                .WithParameter(TypedParameter.From(_settingsManager.Nested(x => x.Db.TradeVolumesConnString)))
                .As<ITradeVolumesRepository>()
                .SingleInstance();

            builder.RegisterType<TradeVolumesCalculator>()
                .As<ITradeVolumesCalculator>()
                .SingleInstance()
                .WithParameter("warningDelay", TimeSpan.FromHours(1));

            builder.RegisterType<TradelogSubscriber>()
                .As<IStartStop>()
                .SingleInstance()
                .WithParameter("connectionString", settings.Rabbit.ConnectionString)
                .WithParameter("exchangeName", settings.Rabbit.ExchangeName);
        }
    }
}
