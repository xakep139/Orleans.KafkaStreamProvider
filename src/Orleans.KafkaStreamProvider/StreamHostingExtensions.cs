using System;

using Orleans.Configuration;
using Orleans.Hosting;

namespace Orleans.Providers.Streams.KafkaQueue
{
    public static class StreamHostingExtensions
    {
        /// <summary>
        /// Configure silo to use KafkaStreamProvider
        /// </summary>
        public static ISiloHostBuilder AddKafkaStreamProvider(this ISiloHostBuilder builder,
                                                              string name,
                                                              Action<OptionsBuilder<KafkaStreamProviderOptions>> configureOptions = null)

        {
            return builder.AddPersistentStreams(name, KafkaQueueAdapterFactory.Create, c => c.Configure(configureOptions));
        }
    }
}