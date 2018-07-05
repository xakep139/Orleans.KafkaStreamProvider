using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.KafkaQueue
{
    [Serializable]
    public class KafkaBatchContainer : IBatchContainer
    {
        private readonly IEnumerable<string> _events;

        private readonly Dictionary<string, object> _requestContext;

        public Guid StreamGuid { get; }
        public string StreamNamespace { get; }
        public StreamSequenceToken SequenceToken => TopicPartitionOffset != null ? new EventSequenceToken(TopicPartitionOffset.Offset) : null;
        public TopicPartitionOffset TopicPartitionOffset { get; }
        public bool WasFetched { get; private set; }

        private KafkaBatchContainer(string streamNamespace, IEnumerable<string> events, TopicPartitionOffset topicPartitionOffset)
        {
            _events = events;
            TopicPartitionOffset = topicPartitionOffset;
            StreamNamespace = streamNamespace;
        }

        public KafkaBatchContainer(Guid streamId, string streamNamespace, IReadOnlyList<string> events, Dictionary<string, object> requestContext)
        {
            _events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events");

            StreamGuid = streamId;
            StreamNamespace = streamNamespace;
            _requestContext = requestContext;
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            WasFetched = true;
            return _events.OfType<T>()
                          .Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, new EventSequenceToken(TopicPartitionOffset.Offset, i)));
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc) =>
            _events.Any(item => shouldReceiveFunc(stream, filterData, item));

        public bool ImportRequestContext()
        {
            if (_requestContext != null)
            {
                RequestContextExtensions.Import(_requestContext);
                return true;
            }

            return false;
        }

        internal static IReadOnlyCollection<IBatchContainer> FromKafkaMessage(IReadOnlyCollection<Message<string, string>> messages, string streamNamespace) =>
            messages.GroupBy(m => (m.Topic, m.Partition))
                    .Select(
                        group => new KafkaBatchContainer(
                            streamNamespace,
                            group.Select(m => m.Value),
                            new TopicPartitionOffset(new TopicPartition(group.Key.Topic, group.Key.Partition), group.Max(g => g.Offset.Value))))
                    .ToList();
    }
}