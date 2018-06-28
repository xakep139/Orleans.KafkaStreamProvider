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
        private readonly List<object> _events;

        private readonly Dictionary<string, object> _requestContext;

        public Guid StreamGuid { get; }
        public string StreamNamespace { get; }
        public StreamSequenceToken SequenceToken { get; }
        public string Timestamp { get; }
        public TopicPartitionOffset TopicPartitionOffset { get; set; }

        public KafkaBatchContainer(Guid streamId, string streamNamespace, List<object> events, Dictionary<string, object> requestContext, StreamSequenceToken token)
        {
            _events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events");

            StreamGuid = streamId;
            SequenceToken = token;
            StreamNamespace = streamNamespace;
            _requestContext = requestContext;
            Timestamp = DateTime.UtcNow.ToString("O");
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
            => _events.OfType<T>()
                      .Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, new EventSequenceToken(TopicPartitionOffset.Offset, i)));

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
    }
}