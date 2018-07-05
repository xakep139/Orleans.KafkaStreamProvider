using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Orleans.Providers.Streams.KafkaQueue
{
    public class KafkaQueueAdapter : IQueueAdapter
    {
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<KafkaQueueAdapter> _logger;
        private readonly KafkaStreamProviderOptions _options;
        private readonly KafkaMessageSender _producer;

        public bool IsRewindable => true;

        public string Name { get; }

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public KafkaQueueAdapter(
            HashRingBasedStreamQueueMapper queueMapper,
            KafkaStreamProviderOptions options,
            string providerName,
            ILoggerFactory loggerFactory)
        {
            if (string.IsNullOrEmpty(providerName))
            {
                throw new ArgumentNullException(nameof(providerName));
            }

            _options = options ?? throw new ArgumentNullException(nameof(options));
            _streamQueueMapper = queueMapper ?? throw new ArgumentNullException(nameof(queueMapper));
            Name = providerName;
            _loggerFactory = loggerFactory;
            _logger = loggerFactory.CreateLogger<KafkaQueueAdapter>();

            _producer = new KafkaMessageSender(loggerFactory, options);

            _logger.LogInformation("{0} - Created", nameof(KafkaQueueAdapter));
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId) => new KafkaQueueAdapterReceiver(Name, queueId, _options, _loggerFactory);

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            if (token != null)
            {
                throw new NotSupportedException("Cannot queue message with custom sequence token");
            }

            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);

            var partitionId = (int)queueId.GetNumericId();

            _logger.LogDebug("For StreamId: {0}, StreamNamespace: {1} using partition {2}", streamGuid, streamNamespace, partitionId);

            foreach (var @event in events.OfType<string>())
            {
                await _producer.SendAsync(_options.TopicName, @event, partitionId);
            }
        }
    }
}