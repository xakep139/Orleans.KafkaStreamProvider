using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.KafkaStreamProvider.KafkaQueue
{
    public class KafkaQueueAdapterReceiver : KafkaConsumerWrapper, IQueueAdapterReceiver
    {
        private Task _currentCommitTask;
        private readonly ILogger<KafkaQueueAdapterReceiver> _logger;
        private readonly SerializationManager _serializationManager;
        private readonly KafkaStreamProviderOptions _options;

        private readonly BufferBlock<Message<Null, byte[]>>
            _messageQueue = new BufferBlock<Message<Null, byte[]>>();

        private bool _partitionEofReached;

        public QueueId Id { get; }

        public long CurrentOffset { get; private set; }

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
            Consumer.Subscribe(_options.TopicName);
            Consumer.OnMessage += OnMessage;
            Consumer.OnPartitionEOF += OnPartitionEof;

            return Task.CompletedTask;
        }

        private void OnPartitionEof(object sender, TopicPartitionOffset e) => _partitionEofReached = true;

        private void OnMessage(object sender, Message<Null, byte[]> message) => _messageQueue.Post(message);

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            IList<IBatchContainer> batches = new List<IBatchContainer>(maxCount);
            while (!_partitionEofReached && _messageQueue.Count < maxCount)
            {
                Consumer.Poll(100);
            }

            while (await _messageQueue.OutputAvailableAsync())
            {
                var message = await _messageQueue.ReceiveAsync();
                batches.Add(FromKafkaMessage(_serializationManager, message));
            }

            // No batches, we are done here..
            if (batches.Count <= 0)
            {
                return batches;
            }

            _logger.Debug("Pulled {0} messages for queue number {1}", batches.Count, Id.GetNumericId());
            CurrentOffset += batches.Count;

            return batches;
        }

        private IBatchContainer FromKafkaMessage(SerializationManager serializationManager, Message<Null, byte[]> message)
        {
            var kafkaBatch = serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(message.Value);
            kafkaBatch.TopicPartitionOffset = message.TopicPartitionOffset;

            return kafkaBatch;
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            if (_currentCommitTask != null)
            {
                await Task.WhenAny(_currentCommitTask, Task.Delay(timeout));
                _currentCommitTask = null;

                Consumer.Unsubscribe();
                Consumer.OnMessage -= OnMessage;
                Consumer.OnPartitionEOF -= OnPartitionEof;
                Dispose();

                _logger.Debug("The receiver had finished a commit and was shutted down");
            }
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            // Finding the highest offset
            if (messages.Any())
            {
                var lastBatch = messages.OfType<KafkaBatchContainer>()
                    .OrderByDescending(m => m.SequenceToken)
                    .First();

                await Consumer.CommitAsync(new[] {lastBatch.TopicPartitionOffset});

                _logger.Debug("Committed an offset to the ConsumerGroup. ConsumerGroup is {0}, TopicPartitionOffset is {1}",
                    _options.ConsumerGroupName, lastBatch.TopicPartitionOffset);
            }
        }
    }
}