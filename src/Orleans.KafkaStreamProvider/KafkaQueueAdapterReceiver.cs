using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Providers.Streams.KafkaQueue
{
    public class KafkaQueueAdapterReceiver : KafkaConsumerWrapper, IQueueAdapterReceiver
    {
        private readonly ILogger<KafkaQueueAdapterReceiver> _logger;
        private readonly SerializationManager _serializationManager;
        private readonly KafkaStreamProviderOptions _options;

        private readonly Queue<Message<Null, byte[]>>
            _messageQueue = new Queue<Message<Null, byte[]>>();

        private bool _partitionEofReached;

        public QueueId Id { get; }

        public KafkaQueueAdapterReceiver(
            SerializationManager serializationManager,
            QueueId queueId,
            KafkaStreamProviderOptions options,
            ILoggerFactory loggerFactory)
            : base(loggerFactory.CreateLogger<KafkaConsumerWrapper>(), options)
        {
            Id = queueId ?? throw new ArgumentNullException(nameof(queueId));
            _options = options ?? throw new ArgumentNullException(nameof(options));

            _serializationManager = serializationManager;
            _logger = loggerFactory.CreateLogger<KafkaQueueAdapterReceiver>();
        }

        public Task Initialize(TimeSpan timeout)
        {
            Consumer.OnMessage += OnMessage;
            Consumer.OnPartitionEOF += OnPartitionEof;
            Consumer.Subscribe(_options.TopicName);

            return Task.CompletedTask;
        }

        private void OnPartitionEof(object sender, TopicPartitionOffset e) => _partitionEofReached = true;

        private void OnMessage(object sender, Message<Null, byte[]> message) => _messageQueue.Enqueue(message);

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            _partitionEofReached = false;
            //while (!_partitionEofReached && _messageQueue.Count < maxCount)
            {
                Consumer.Poll(0);
            }

            IList<IBatchContainer> batches = new List<IBatchContainer>();
            if (_messageQueue.Count < 1)
            {
                return Task.FromResult(batches);
            }

            while (_messageQueue.Count > 0)
            {
                batches.Add(FromKafkaMessage(_serializationManager, _messageQueue.Dequeue()));
            }

            _logger.Debug("Pulled {0} messages for queue number {1} from topic {topic}", batches.Count, Id.GetNumericId(), _options.TopicName);

            return Task.FromResult(batches);
        }

        private static IBatchContainer FromKafkaMessage(SerializationManager serializationManager, Message<Null, byte[]> message)
        {
            var kafkaBatch = serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(message.Value);
            kafkaBatch.TopicPartitionOffset = message.TopicPartitionOffset;

            return kafkaBatch;
        }

        public Task Shutdown(TimeSpan timeout)
        {
            Consumer.Unsubscribe();
            Consumer.OnMessage -= OnMessage;
            Consumer.OnPartitionEOF -= OnPartitionEof;
            Dispose();

            _logger.Debug("The receiver was shutted down");

            return Task.CompletedTask;
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            if (messages.Count > 0)
            {
                // Finding the highest offset
                var lastBatch = messages.OfType<KafkaBatchContainer>()
                    .OrderByDescending(m => m.TopicPartitionOffset.Offset.Value)
                    .First();

                var topicPartitionOffset = lastBatch.TopicPartitionOffset;
                await Consumer.CommitAsync(new[] { new TopicPartitionOffset(topicPartitionOffset.TopicPartition, topicPartitionOffset.Offset + 1) });

                _logger.Debug("Committed an offset to the ConsumerGroup {0}, TopicPartitionOffset: {1}",
                    _options.ConsumerGroup, topicPartitionOffset);
            }
        }
    }
}