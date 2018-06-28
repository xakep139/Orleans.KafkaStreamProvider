using System;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Orleans.Providers.Streams.KafkaQueue.TimedQueueCache
{
    public class TimedQueueAdapterCache : IQueueAdapterCache
    {
        private readonly TimeSpan _cacheTimeSpan;
        private readonly int _cacheNumOfBuckets;
        private readonly ILogger _logger;

        public TimedQueueAdapterCache(TimeSpan cacheTimeSpan, int cacheSize, int cacheNumOfBuckets, ILoggerFactory loggerFactory)
        {
            if (cacheTimeSpan == TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(cacheTimeSpan), "cacheTimeSpan must be larger than zero TimeSpan.");
            }

            _cacheTimeSpan = cacheTimeSpan;
            Size = cacheSize;
            _cacheNumOfBuckets = cacheNumOfBuckets;
            _logger = loggerFactory.CreateLogger<TimedQueueAdapterCache>();
        }

        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            return new TimedQueueCache(queueId, _cacheTimeSpan, Size, _cacheNumOfBuckets, _logger);
        }

        public int Size { get; }
    }
}
