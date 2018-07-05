namespace Orleans.Providers.Streams.KafkaQueue
{
    public class KafkaStreamProviderOptions
    {
        public string TopicName { get; set; }
        public string BrokerEndpoints { get; set; }
        public int PartitionsCount { get; set; } = 1;
        public int ContainerBatchSize { get; set; } = 100;
        public ConsumerOptions Consumer { get; set; }
        public ProducerOptions Producer { get; set; }
        public CacheOptions Cache { get; set; }

        public sealed class CacheOptions
        {
            public int CacheSize { get; set; } = 4096 * 4;
            public int CacheTimespanSeconds { get; set; } = 60;
            public int CacheNumOfBuckets { get; set; } = 10;
        }

        public sealed class ConsumerOptions
        {
            public string ConsumerGroup { get; set; }
            public int ConsumeTimeoutMs { get; set; } = 100;
            public bool EnableAutoCommit { get; set; } = false;
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
