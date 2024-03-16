
using Confluent.Kafka;
using Creos.KafkaHelper.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Creos.KafkaHelper.HostedServices
{
    internal class ComsumerCommitterService : BackgroundService
    {
        private readonly ILogger<ComsumerCommitterService> _logger;
        private IEnumerable<ConsumerMember> _consumerMembers;
        private readonly IServiceProvider _serviceProvider;
        public ComsumerCommitterService(ILogger<ComsumerCommitterService> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _logger.LogTrace("KafkaHelper | ComsumerCommitterService.StartAsync");
            await CommitMessagesAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task CommitMessagesAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(millisecondsDelay: 1000 * 5, cancellationToken).ConfigureAwait(false);
            _consumerMembers = _serviceProvider.GetServices<ConsumerMember>();
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    foreach (var consumerMember in _consumerMembers.Where(x => x.ConsumerModel.Active && !x.ConsumerModel.EnableAutoCommit))
                    {
                        if (consumerMember.TopicPartitionOffsets_ToCommit.Count >= consumerMember.ConsumerModel.BatchOffsetsToCommit || (!consumerMember.TopicPartitionOffsets_ToCommit.IsEmpty && consumerMember.DateTimeLastCommit < DateTime.Now.AddMilliseconds(-(int)consumerMember.ConsumerModel.FrequencyToCommitMs)))
                        {
                            var topicPartitionOffsetList = new List<TopicPartitionOffset>(consumerMember.TopicPartitionOffsets_ToCommit);
                            consumerMember.TopicPartitionOffsets_ToCommit.Clear();

                            topicPartitionOffsetList = new List<TopicPartitionOffset>(topicPartitionOffsetList.GroupBy(t => t.TopicPartition, (key, tpo) => tpo.OrderByDescending(x => x.Offset.Value).First()).Select(tpo => new TopicPartitionOffset(tpo.TopicPartition, new Offset(tpo.Offset.Value + 1))).ToList());
                            consumerMember.ConsumerInstance.Commit(topicPartitionOffsetList);
                            consumerMember.DateTimeLastCommit = DateTime.Now;
                        }
                    }
                    await Task.Delay(millisecondsDelay: 100, cancellationToken).ConfigureAwait(false);
                }
                catch (TaskCanceledException) { }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "KafkaHelper | ConsumerCommitterService.CommitMessagesAsync");
                }
            }
        }

    }
}
