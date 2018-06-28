namespace Orleans.KafkaStreamProvider.KafkaQueue
{
    public class KafkaStreamProviderOptions
    {
        public string TopicName { get; set; }
        public string ConsumerGroupName { get; set; }
        public string BrokerEndpoints { get; set; }
        public ConsumerOptions Consumer { get; set; }
        public ProducerOptions Producer { get; set; }
        public int NumOfQueues { get; set; } = 1;
        public int CacheSize { get; set; } = 4096 * 4;
        public int CacheTimespanInSeconds { get; set; } = 60;
        public int CacheNumOfBuckets { get; set; } = 10;

        public sealed class ConsumerOptions
        {
            public bool EnableAutoCommit { get; set; }
            public int FetchWaitMaxMs { get; set; }
            public int FetchErrorBackoffMs { get; set; }
            public int FetchMessageMaxBytes { get; set; }
            public int QueuedMinMessages { get; set; }
        }

        public sealed class ProducerOptions
        {
            public int QueueBufferingMaxMs { get; set; }
            public int QueueBufferingMaxKbytes { get; set; }
            public int BatchNumMessages { get; set; }
            public int MessageMaxBytes { get; set; }
        }
    }
}
