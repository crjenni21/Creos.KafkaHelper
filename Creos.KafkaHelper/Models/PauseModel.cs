using Confluent.Kafka;

namespace Creos.KafkaHelper.Models
{
    internal sealed class PauseModel
    {
        public List<TopicPartition> TopicPartitions { get; set; } = new List<TopicPartition>();
        public CancellationToken CancellationTokenForResumeTask { get; set; }
        public Task ResumeTask { get; set; }
        public Guid ResumeGuid { get; set; }
        public bool CommitPendingMessages { get; set; }
    }
}
