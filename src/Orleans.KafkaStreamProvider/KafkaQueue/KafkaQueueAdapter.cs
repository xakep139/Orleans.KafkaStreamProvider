using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.KafkaStreamProvider.KafkaQueue
{
    public class KafkaQueueAdapter : IQueueAdapter
    {
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<KafkaQueueAdapter> _logger;
        private readonly SerializationManager _serializationManager;
        private readonly KafkaStreamProviderOptions _options;
        private readonly KafkaMessageSender _producer;
    
        public bool IsRewindable => true;

        public string Name { get; }

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public KafkaQueueAdapter(SerializationManager serializationManager,
            HashRingBasedStreamQueueMapper queueMapper,
            KafkaStreamProviderOptions options,
            string providerName,
            ILoggerFactory loggerFactory)
        {
            if (string.IsNullOrEmpty(providerName))
            {
                throw new ArgumentNullException(nameof(providerName));
            }

            _serializationManager = serializationManager;
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _streamQueueMapper = queueMapper ?? throw new ArgumentNullException(nameof(queueMapper));
            Name = providerName;
            _loggerFactory = loggerFactory;
            _logger = loggerFactory.CreateLogger<KafkaQueueAdapter>();

            _producer = new KafkaMessageSender(loggerFactory, options);

            _logger.Info("{0} - Created", nameof(KafkaQueueAdapter));
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId) =>
            new KafkaQueueAdapterReceiver(_serializationManager, queueId, _options, _loggerFactory);

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            
            var partitionId = (int)queueId.GetNumericId();

            _logger.Debug("For StreamId: {0}, StreamNamespace:{1} using partition {2}", streamGuid, streamNamespace, partitionId);

            var container = new KafkaBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext, token);
            var payload = _serializationManager.SerializeToByteArray(container);

            if (payload == null)
            {
                _logger.Info("The batch factory returned a faulty message, the message was not sent");
                return;
            }

            await _producer.SendAsync(_options.TopicName, payload, partitionId);
        }
    }
}