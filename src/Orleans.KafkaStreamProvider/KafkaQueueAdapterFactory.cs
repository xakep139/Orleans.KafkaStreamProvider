using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.KafkaQueue
{
    public class KafkaQueueAdapterFactory : IQueueAdapterFactory, IStreamFailureHandler
    {
        private readonly KafkaStreamProviderOptions _options;
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        private readonly IQueueAdapterCache _adapterCache;
        private readonly string _providerName;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<KafkaQueueAdapterFactory> _logger;

        public KafkaQueueAdapterFactory(
            string name,
            KafkaStreamProviderOptions options,
            ILoggerFactory loggerFactory)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            _options = options ?? throw new ArgumentNullException(nameof(options));
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            _logger = _loggerFactory.CreateLogger<KafkaQueueAdapterFactory>();
            _providerName = name;
            _streamQueueMapper = new HashRingBasedStreamQueueMapper(
                new HashRingStreamQueueMapperOptions { TotalQueueCount = _options.PartitionsCount },
                name);

            _adapterCache = new SimpleQueueAdapterCache(new SimpleQueueCacheOptions { CacheSize = options.Cache.CacheSize }, _providerName, _loggerFactory);
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new KafkaQueueAdapter(_streamQueueMapper, _options, _providerName, _loggerFactory);
            return Task.FromResult<IQueueAdapter>(adapter);
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) =>
            Task.FromResult<IStreamFailureHandler>(this);

        public IQueueAdapterCache GetQueueAdapterCache() => _adapterCache;

        public IStreamQueueMapper GetStreamQueueMapper() => _streamQueueMapper;

        public static IQueueAdapterFactory Create(IServiceProvider services, string name)
        {
            var options = services.GetOptionsByName<KafkaStreamProviderOptions>(name);
            var factory = ActivatorUtilities.CreateInstance<KafkaQueueAdapterFactory>(services, name, options);
            return factory;
        }

        public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamIdentity, StreamSequenceToken sequenceToken)
        {
            _logger.LogCritical(
                "Delivery failure. Subscription: {subscriptionId}, stream provider: {streamProviderName}, stream identity: {streamIdentity}, sequence token: {sequenceToken}",
                subscriptionId,
                streamProviderName,
                streamIdentity,
                sequenceToken);

            return Task.CompletedTask;
        }

        public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamIdentity, StreamSequenceToken sequenceToken)
        {
            _logger.LogError(
                "Subscription failure. Subscription: {subscriptionId}, stream provider: {streamProviderName}, stream identity: {streamIdentity}, sequence token: {sequenceToken}",
                subscriptionId,
                streamProviderName,
                streamIdentity,
                sequenceToken);

            return Task.CompletedTask;
        }

        public bool ShouldFaultSubsriptionOnError { get; } = true;
    }
}