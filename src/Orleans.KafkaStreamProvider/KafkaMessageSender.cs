using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Microsoft.Extensions.Logging;

namespace Orleans.Providers.Streams.KafkaQueue
{
    public class KafkaMessageSender : IDisposable
    {
        private readonly ILogger<KafkaMessageSender> _logger;
        private readonly Producer<Null, byte[]> _producer;

        public KafkaMessageSender(ILoggerFactory loggerFactory, KafkaStreamProviderOptions options)
        {
            _logger = loggerFactory.CreateLogger<KafkaMessageSender>();

            var producerConfig = new Dictionary<string, object>
                {
                    // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                    { "bootstrap.servers", options.BrokerEndpoints },
                    { "api.version.request", true },
                    { "queue.buffering.max.ms", options.Producer.QueueBufferingMaxMs },
                    { "queue.buffering.max.kbytes", options.Producer.QueueBufferingMaxKbytes },
                    { "batch.num.messages", options.Producer.BatchNumMessages },
                    { "message.max.bytes", options.Producer.MessageMaxBytes },
#if DEBUG
                    { "debug", "msg" },
                    { "socket.blocking.max.ms", 1 }, // https://github.com/edenhill/librdkafka/wiki/How-to-decrease-message-latency
#else
                    { "log.connection.close", false },
#endif
                    {
                        "default.topic.config",
                        new Dictionary<string, object>
                            {
                                { "message.timeout.ms", 5000 },
                                { "request.required.acks", -1 }
                            }
                    }
                };
            _producer = new Producer<Null, byte[]>(producerConfig, new NullSerializer(), new ByteArraySerializer());
            _producer.OnLog += OnLog;
            _producer.OnError += OnLogError;
            _producer.OnStatistics += OnStatistics;
        }

        public async Task SendAsync(string topic, byte[] message, int partition)
        {
            try
            {
                var result = await _producer.ProduceAsync(topic, null, message, partition);
                _logger.LogDebug(
                    "Produced to Kafka. Topic/partition/offset: '{kafkaTopic}/{kafkaPartition}/{kafkaOffset}'.",
                    result.Topic,
                    result.Partition,
                    result.Offset);

                if (result.Error.HasError)
                {
                    throw new KafkaException(result.Error);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    default,
                    ex,
                    "Error producing to Kafka. Topic: '{kafkaTopic}'.",
                    topic);
                throw;
            }
        }

        public void Dispose()
        {
            if (_producer != null)
            {
                _producer.OnLog -= OnLog;
                _producer.OnError -= OnLogError;
                _producer.OnStatistics -= OnStatistics;
                _producer.Dispose();
            }
        }

        private void OnLog(object sender, LogMessage logMessage)
            => _logger.LogDebug(
                "Producing to Kafka. Client: '{kafkaClient}', syslog level: '{kafkaLogLevel}', message: '{kafkaLogMessage}'.",
                logMessage.Name,
                logMessage.Level,
                logMessage.Message);

        private void OnLogError(object sender, Error error)
            => _logger.LogInformation("Producing to Kafka. Client error: '{kafkaError}'. No action required.", error);

        private void OnStatistics(object sender, string json)
            => _logger.LogDebug("Producing to Kafka. Statistics: '{kafkaStatistics}'.", json);
    }
}