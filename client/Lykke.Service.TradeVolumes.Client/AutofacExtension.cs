using System;
using Autofac;
using Common.Log;

namespace Lykke.Service.TradeVolumes.Client
{
    public static class AutofacExtension
    {
        public static void RegisterTradeVolumesClient(this ContainerBuilder builder, string serviceUrl, ILog log)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            if (serviceUrl == null) throw new ArgumentNullException(nameof(serviceUrl));
            if (log == null) throw new ArgumentNullException(nameof(log));
            if (string.IsNullOrWhiteSpace(serviceUrl))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(serviceUrl));

            builder.RegisterType<TradeVolumesClient>()
                .WithParameter("serviceUrl", serviceUrl)
                .As<ITradeVolumesClient>()
                .SingleInstance();
        }

        public static void RegisterTradeVolumesClient(this ContainerBuilder builder, TradeVolumesServiceClientSettings settings, ILog log)
        {
            builder.RegisterTradeVolumesClient(settings?.ServiceUrl, log);
        }
    }
}
