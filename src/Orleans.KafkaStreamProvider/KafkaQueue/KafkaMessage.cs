using Confluent.Kafka;

namespace Orleans.KafkaStreamProvider.KafkaQueue
{
    public class KafkaMessage
    {
        public KafkaMessage(string key, string source, TopicPartitionOffset topicPartitionOffset, Timestamp timestamp)
        {
            Key = key;
            Source = source;
            TopicPartitionOffset = topicPartitionOffset;
            Timestamp = timestamp;
        }

        public string Key { get; }
        public string Source { get; }
        public TopicPartitionOffset TopicPartitionOffset { get; }
        public Timestamp Timestamp { get; }
    }
}