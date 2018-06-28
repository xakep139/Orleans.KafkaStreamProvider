using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.KafkaStreamProvider.KafkaQueue;
using Orleans.Runtime;

namespace Orleans.KafkaStreamProvider
{
    public static class StreamExtensions
    {
        public static IServiceCollection AddKafkaStreamProvider(this IServiceCollection services, string name, Action<OptionsBuilder<KafkaStreamProviderOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<KafkaStreamProviderOptions>(name));

            services.ConfigureNamedOptionForLogging<KafkaStreamProviderOptions>(name);
            return services.AddSingletonNamedService(name, KafkaQueueAdapterFactory.Create);
        }
    }
}