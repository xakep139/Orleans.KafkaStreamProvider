using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Orleans.Providers.Streams.KafkaQueue
{
    public static class StreamExtensions
    {
        public static IServiceCollection AddKafkaStreamProvider(
            this IServiceCollection services,
            string name,
            Action<OptionsBuilder<KafkaStreamProviderOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<KafkaStreamProviderOptions>(name));
            return services.ConfigureNamedOptionForLogging<KafkaStreamProviderOptions>(name)
                .AddSingletonNamedService(name, KafkaQueueAdapterFactory.Create);
        }
    }
}