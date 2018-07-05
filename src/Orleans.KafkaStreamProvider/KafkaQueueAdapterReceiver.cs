using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Orleans.Providers.Streams.KafkaQueue
{
    public class KafkaQueueAdapterReceiver : KafkaConsumerWrapper, IQueueAdapterReceiver
    {
        private readonly string _providerName;
        private readonly KafkaStreamProviderOptions _options;
        private readonly ILogger<KafkaQueueAdapterReceiver> _logger;

        public QueueId Id { get; }

        public KafkaQueueAdapterReceiver(
            string providerName,
            QueueId queueId,
            KafkaStreamProviderOptions options,
            ILoggerFactory loggerFactory)
            : base(loggerFactory.CreateLogger<KafkaConsumerWrapper>(), options)
        {
            Id = queueId ?? throw new ArgumentNullException(nameof(queueId));
            _providerName = providerName;
            _options = options ?? throw new ArgumentNullException(nameof(options));

            _logger = loggerFactory.CreateLogger<KafkaQueueAdapterReceiver>();
        }

        public Task Initialize(TimeSpan timeout)
        {
            Consumer.Assign(new[] { new TopicPartition(_options.TopicName, (int)Id.GetNumericId()) });

            return Task.CompletedTask;
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var queue = new Queue<Message<string, string>>();
            while (queue.Count < maxCount && Consumer.Consume(out var message, _options.Consumer.ConsumeTimeoutMs))
            {
                queue.Enqueue(message);
            }

            var batches = new List<IBatchContainer>();
            if (queue.Count < 1)
            {
                return Task.FromResult((IList<IBatchContainer>)batches);
            }

            var messageCount = queue.Count;
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(
                    "Pulled {count} messages for queue number {queueNumber} from topic {topic}, position: {position}",
                    messageCount,
                    Id.GetNumericId(),
                    _options.TopicName,
                    Consumer.Position(new[] { new TopicPartition(_options.TopicName, (int)Id.GetNumericId()) }));
            }

            var batch = new List<Message<string, string>>(_options.ContainerBatchSize);
            while (queue.Count > 0)
            {
                batch.Add(queue.Dequeue());
                if (batch.Count < _options.ContainerBatchSize)
                {
                    continue;
                }

                batches.AddRange(KafkaBatchContainer.FromKafkaMessage(batch, _providerName));
                batch.Clear();
            }

            if (batch.Count > 0)
            {
                batches.AddRange(KafkaBatchContainer.FromKafkaMessage(batch, _providerName));
            }

            _logger.LogDebug(
                "Constructed {count} batches with {messageCount} messages for queue number {queueNumber} from topic {topic}",
                batches.Count,
                messageCount,
                Id.GetNumericId(),
                _options.TopicName);

            return Task.FromResult((IList<IBatchContainer>)batches);
        }

        public Task Shutdown(TimeSpan timeout)
        {
            Consumer.Unassign();
            Dispose();

            _logger.LogDebug("The receiver was shutted down");

            return Task.CompletedTask;
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            var containers = messages.OfType<KafkaBatchContainer>()
                                     .ToList();

            if (containers.Count > 0 && containers.All(m => m.WasFetched))
            {
                var offsetsToCommit = new List<TopicPartitionOffset>();
                foreach (var group in containers.GroupBy(c => (c.TopicPartitionOffset.Topic, c.TopicPartitionOffset.Partition)))
                {
                    var latestBatchInPartition = group.OrderByDescending(g => g.TopicPartitionOffset.Offset)
                                                           .First();

                    offsetsToCommit.Add(new TopicPartitionOffset(new TopicPartition(group.Key.Topic, group.Key.Partition), latestBatchInPartition.TopicPartitionOffset.Offset + 1));
                }

                await Consumer.CommitAsync(offsetsToCommit);

                _logger.LogDebug("Committed an offsets to the ConsumerGroup {consumerGroup}, TopicPartitionOffsets: {offsetsToCommit}",
                                 _options.Consumer.ConsumerGroup, offsetsToCommit);
            }
        }
    }
}