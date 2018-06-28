using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;

namespace Orleans.Providers.Streams.KafkaQueue
{
    public static class StreamHostingExtensions
    {
        /// <summary>
        /// Configure silo to use SimpleMessageProvider
        /// </summary>
        public static ISiloHostBuilder AddKafkaStreamProvider(this ISiloHostBuilder builder,
                                                                      string name,
                                                                      Action<OptionsBuilder<KafkaStreamProviderOptions>> configureOptions)

        {
            return builder.ConfigureServices(services => services.AddSiloKafkaStreamProvider(name, configureOptions));
        }

        public static void AddSiloKafkaStreamProvider(
            this IServiceCollection services,
            string name,
            Action<OptionsBuilder<KafkaStreamProviderOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<KafkaStreamProviderOptions>(name));
            services.ConfigureNamedOptionForLogging<KafkaStreamProviderOptions>(name)
                    .AddSingletonNamedService(name, KafkaQueueAdapterFactory.Create);
        }
    }
}