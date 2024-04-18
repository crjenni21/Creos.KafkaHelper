
using System.Collections.Concurrent;

namespace Creos.KafkaHelper.Models
{
    internal class KafkaConfigurationModel
    {
        public List<string> Brokers { get; set; }
        public List<ProducerModel> Producers { get; set; } = new List<ProducerModel>();
        public List<ConsumerModel> Consumers { get; set; } = new List<ConsumerModel>();
        public int WildCardRestartTimer_Minutes { get; set; } = 30;
    }

    internal class ProducerMessages
    {
        internal ConcurrentBag<Task> ProducerTasks = new();
    }

    internal class ProducerModel
    {
        private string _producerName = string.Empty;
        public string ProducerName 
        {
            get 
            {
                return _producerName.Trim();
            }
            set 
            {
                _producerName = value;
            } 
        }
        internal List<string> Brokers { get; set; }
        public string Topic { get; set; }
        public bool Active { get; set; } = true;
        public Confluent.Kafka.Partitioner Partitioner { get; set; } = Confluent.Kafka.Partitioner.ConsistentRandom;
        public int LingerMS { get; set; } = 1000;  // 1 second
        public int BatchSizeBytes { get; set; } = 1000000; // 1 MB
        public bool AllowAutoCreateTopics { get; set; } = true;
    }

    public sealed class ConsumerModel
    {
        public string Name
        {
            get
            {
                return _name.Trim();
            }
        }
        private string _name = string.Empty;
        public string ConsumerName
        {
            get
            {
                return _name.Trim();
            }
            internal set
            {
                _name = value;
            }
        }
        internal bool EnableAutoCommit { get; set; } = true;
        internal List<string> Brokers { get; set; }
        internal int BatchOffsetsToCommit { get; set; } = 500;
        internal int FrequencyToCommitMs { get; set; } = 1000;

        private string _groupID = string.Empty;
        internal string GroupID
        {
            get
            {
                return _groupID.Trim().Replace(' ', '_');
            }
            set
            {
                _groupID = value;
            }
        }
        internal List<string> Topics { get; set; }
        public bool Active { get; internal set; } = true;
        internal string ConsumeFromDate { get; set; }
        internal int? StatisticsIntervalMs { get; set; }
        internal int? SessionTimeoutMs { get; set; }
        internal bool? EnablePartitionEof { get; set; }
        internal int? ReconnectBackoffMs { get; set; }
        internal int? HeartbeatIntervalMs { get; set; }
        internal int? ReconnectBackoffMaxMs { get; set; }
        internal int? FetchMaxBytes { get; set; }
        internal int? AutoCommitIntervalMs { get; set; }

    }

    public sealed class ConsumerAccessorModel
    {
        public bool IsActive { get; internal set; }
        public DateTime DateTimeLastCommit { get; internal set; }
        public ConsumerModel ConsumerModel { get; internal set; }
    }
}
