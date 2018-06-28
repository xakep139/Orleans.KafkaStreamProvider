using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.KafkaQueue.TimedQueueCache;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Providers.Streams.KafkaQueue
{
    public class KafkaQueueAdapterFactory : IQueueAdapterFactory
    {
        private readonly KafkaStreamProviderOptions _options;
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        private readonly IQueueAdapterCache _adapterCache;
        private readonly SerializationManager _serializationManager;
        private readonly string _providerName;
        private readonly ILoggerFactory _loggerFactory;

        public KafkaQueueAdapterFactory(
            string name,
            KafkaStreamProviderOptions options,
            SerializationManager serializationManager,
            ILoggerFactory loggerFactory)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            _serializationManager = serializationManager;
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            _providerName = name;
            _streamQueueMapper = new HashRingBasedStreamQueueMapper(
                new HashRingStreamQueueMapperOptions {TotalQueueCount = _options.NumOfQueues},
                name);

            _adapterCache = new TimedQueueAdapterCache(TimeSpan.FromSeconds(_options.CacheTimespanInSeconds), _options.CacheSize, _options.CacheNumOfBuckets, _loggerFactory);
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new KafkaQueueAdapter(_serializationManager, _streamQueueMapper, _options, _providerName, _loggerFactory);
            return Task.FromResult<IQueueAdapter>(adapter);
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) =>
            Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler(false));

        public IQueueAdapterCache GetQueueAdapterCache() => _adapterCache;

        public IStreamQueueMapper GetStreamQueueMapper() => _streamQueueMapper;

        public static IQueueAdapterFactory Create(IServiceProvider services, string name)
        {
            var options = services.GetOptionsByName<KafkaStreamProviderOptions>(name);
            var factory = ActivatorUtilities.CreateInstance<KafkaQueueAdapterFactory>(services, name, options);
            return factory;
        }
    }
}