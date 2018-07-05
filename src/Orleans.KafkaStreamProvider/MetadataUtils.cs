using System;
using System.Collections.Generic;
using System.Linq;

using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Orleans.Providers.Streams.KafkaQueue
{
    public static class MetadataUtils
    {
        public static TopicMetadata GetTopicMetadata(string brokerEndpoint, string topic)
        {
            if (string.IsNullOrEmpty(brokerEndpoint))
            {
                throw new ArgumentException("Broker endpoint is not set", nameof(brokerEndpoint));
            }

            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentException("Topic is not set", nameof(topic));
            }

            var producerConfig = new Dictionary<string, object>
                {
                    { "bootstrap.servers", brokerEndpoint }
                };

            using (var producer = new Producer<Null, Null>(producerConfig, new NullSerializer(), new NullSerializer()))
            {
                return producer.GetMetadata(topic: topic, allTopics: false)
                               .Topics
                               .First();
            }
        }
    }
}