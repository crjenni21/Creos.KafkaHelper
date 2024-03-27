
using Confluent.Kafka;
using Creos.KafkaHelper.Helper;
using Creos.KafkaHelper.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace Creos.KafkaHelper.Consumer
{
    public interface IConsumerMember
    {
        Task RegisterConsumerMemberAsync(CancellationToken cancellationToken);
        Task ExecuteConsumingAsync(CancellationToken cancellationToken);
        IConsumer<string, string> ConsumerInstance { get; }
        bool ConsumerIsActive { get; }
        ConsumerModel ConsumerModel { get; }

        event AsyncEventHandler<bool> ConsumeEvent;
    }

    public class ConsumerMember : IConsumerMember, IDisposable
    {
        private readonly ILogger<ConsumerMember> _logger;
        
        private readonly IKafkaHelperFunctions _helper;
        private readonly ConcurrentDictionary<Guid, PauseModel> pauseModels = new();
        internal readonly ConcurrentBag<TopicPartitionOffset> TopicPartitionOffsets_ToCommit;
        public IConsumer<string, string> ConsumerInstance { get; private set; }

        internal ConsumerMember(IServiceProvider serviceProvider, ConsumerModel consumerModel)
        {
            _logger = serviceProvider.GetRequiredService<ILogger<ConsumerMember>>();
            ConsumerModel = consumerModel;
            _helper = serviceProvider.GetRequiredService<IKafkaHelperFunctions>();
            TopicPartitionOffsets_ToCommit = new ConcurrentBag<TopicPartitionOffset>();
            ConsumerInstance = _helper.BuildConsumer(consumerModel);
            _logger.LogTrace("Consumer Name: {ConsumerName} Instantiated.", ConsumerModel.ConsumerName);
        }

        public ConsumerModel ConsumerModel { get; private set; }
        public event AsyncEventHandler<bool> ConsumeEvent;
        
        public bool ConsumerIsActive { get; private set; } = false;
    
        internal List<string> Topics { get; set; }
        
        internal DateTime DateTimeLastCommit { get; set; }

        public async Task RegisterConsumerMemberAsync(CancellationToken cancellationToken)
        {
            var topics = new List<string>();

            foreach (var topic in ConsumerModel.Topics.Select(s => s.Trim()))
            {
                if (string.IsNullOrWhiteSpace(topic))
                    continue;

                if (!topics.Contains(topic))
                {
                    if (topic.Contains('*') || topic.Contains('+'))
                    {
                        topics.AddRange(await _helper.GetTopicsFromWildcardAsync(topic).ConfigureAwait(false));
                    }
                    else
                        topics.Add(topic);
                }
            }

            _logger.LogDebug("KafkaHelper | Consumer Name: {Name}, Subscribing to {TopicList}", ConsumerModel.ConsumerName, string.Join(",", topics.Distinct()));

            Topics = topics.Distinct().ToList();

            if ((!string.IsNullOrWhiteSpace(ConsumerModel.ConsumeFromDate)) && DateTime.TryParse(ConsumerModel.ConsumeFromDate, out DateTime consumeFromDate))
            {
                var consumeFromDateForTimestamp = new DateTime(year: consumeFromDate.Year, month: consumeFromDate.Month, day: consumeFromDate.Day, hour: consumeFromDate.Hour, minute: consumeFromDate.Minute, second: consumeFromDate.Second, DateTimeKind.Utc);
                var consumeFromDateTimestamp = new Timestamp(consumeFromDateForTimestamp, TimestampType.CreateTime);

                var config = new AdminClientConfig { BootstrapServers = _helper.BrokerList };
                using var adminClient = new AdminClientBuilder(config).Build();

                var topicPartitionTimestamps = new List<TopicPartitionTimestamp>();
                var topicPartitionOffsets = new List<TopicPartitionOffset>();

                foreach (var topic in topics)
                {
                    topicPartitionTimestamps.Clear();
                    var metaData = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(30));

                    foreach (var topicMetaData in metaData.Topics)  // this should only loop through once for each topic.
                    {
                        _logger.LogDebug("KafkaHelper | ConsumeFromDate: {ConsumeFromDate}, Topic: {Topic}, TopicMetaData.Partitions.Count: {Count}", consumeFromDate, topic, topicMetaData.Partitions.Count);
                        if (topicMetaData.Topic.Equals(topic, StringComparison.OrdinalIgnoreCase))
                        {
                            foreach (var partitionMetaData in topicMetaData.Partitions.OrderBy(x => x.PartitionId))
                            {
                                var topicPartition = new TopicPartition(topicMetaData.Topic, partitionMetaData.PartitionId);
                                topicPartitionTimestamps.Add(new TopicPartitionTimestamp(new TopicPartition(topicMetaData.Topic, partitionMetaData.PartitionId), consumeFromDateTimestamp));
                            }
                        }
                        else
                        {
                            _logger.LogWarning("KafkaHelper | ConsumeFromDate: {onsumeFromDate}, Invalid topic:  {Topic} and {TopicMetaData_Topic}", consumeFromDate, topic, topicMetaData.Topic);
                        }
                    }
                    if (topicPartitionTimestamps.Count > 0)
                    {
                        topicPartitionOffsets.AddRange(ConsumerInstance.OffsetsForTimes(topicPartitionTimestamps, TimeSpan.FromSeconds(30)));
                    }
                }

                var newOffsets = new List<TopicPartitionOffset>();

                foreach (var topicPartitionOffset in topicPartitionOffsets)
                {
                    _logger.LogDebug("KafkaHelper | OffsetsForTimes -- ConsumeFromDate: {consumeFromDate}, Topic: {topicPartitionOffset.Topic}, Partition: {topicPartitionOffset.Partition}, Offset: {topicPartitionOffset.Offset.Value}", consumeFromDate, topicPartitionOffset.Topic, topicPartitionOffset.Partition, topicPartitionOffset.Offset.Value);
                    if (topicPartitionOffset?.Offset == null || topicPartitionOffset.Offset == Offset.End)
                    {
                        var offset = ConsumerInstance.GetWatermarkOffsets(topicPartitionOffset.TopicPartition).High;
                        _logger.LogDebug("KafkaHelper | WatermarkOffset: {offset.Value}, ConsumeFromDate: {consumeFromDate}, Topic: {topicPartitionOffset.Topic}, Partition: {topicPartitionOffset.Partition.Value}", offset.Value, consumeFromDate, topicPartitionOffset.Topic, topicPartitionOffset.Partition.Value);
                        if (offset == Offset.Unset)
                        {
                            offset = ConsumerInstance.QueryWatermarkOffsets(topicPartitionOffset.TopicPartition, TimeSpan.FromSeconds(30)).High;
                            _logger.LogDebug("KafkaHelper | QueryWatermarkOffset: {offset.Value}, ConsumeFromDate: {consumeFromDate}, Topic: {topicPartitionOffset.Topic}, Partition: {topicPartitionOffset.Partition.Value}", offset.Value, consumeFromDate, topicPartitionOffset.Topic, topicPartitionOffset.Partition.Value);
                        }
                        newOffsets.Add(new TopicPartitionOffset(topicPartitionOffset.TopicPartition, offset));
                    }
                    else
                    {
                        newOffsets.Add(new TopicPartitionOffset(topicPartitionOffset.TopicPartition, new Offset(topicPartitionOffset.Offset)));
                    }
                }

                foreach (var offset in newOffsets)
                {
                    _logger.LogDebug("KafkaHelper | Specific Offsets:  ConsumeFromDate: {consumeFromDate}, Topic: {offset.Topic}, Partition: {offset.Partition.Value}, Committing offset: {offset.Offset}", consumeFromDate, offset.Topic, offset.Partition.Value, offset.Offset);
                }

                if (newOffsets.Count > 0)
                {
                    ConsumerInstance.Commit(newOffsets);
                }
            }

            ConsumerIsActive = true;
            ConsumerInstance.Subscribe(Topics);

            var task = Task.Run(async () =>
            {
                try
                {
                    await ExecuteConsumingAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "KafkaHelper | ConsumerWorker.ExecuteConsumer | ConsumerName: {ConsumerName}", ConsumerModel.ConsumerName);
                }

            }, cancellationToken);

        }

        public async Task ExecuteConsumingAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                long offset = 0;
                ConsumeResult<string, string> consumeResult = null;
                try
                {
                    consumeResult = ConsumerInstance.Consume(cancellationToken);
                    if (!consumeResult.IsPartitionEOF)
                    {
                        if (!string.IsNullOrWhiteSpace(consumeResult?.Message?.Value)) // A null value could be a "tombstone record" and may should be handled differently. 
                        {
                            offset = consumeResult.Offset.Value;

                            bool result = await ConsumeEvent(new ConsumeTriggerEventArgs(consumeResult)).ConfigureAwait(false);
                            if (!result)
                            {
                                throw new Exception("KafkaHelper | Consumer Failed. RaiseConsumerTriggered returned false.");
                            }
                        }
                        if (!ConsumerModel.EnableAutoCommit)
                        {
                            var doNotCommit = pauseModels.Any(pc => !pc.Value.CommitPendingMessages && pc.Value.TopicPartitions.Any(tp => tp.Equals(consumeResult.TopicPartition)));
                            if (!doNotCommit)
                            {
                                TopicPartitionOffsets_ToCommit.Add(consumeResult.TopicPartitionOffset);
                            } else
                            {
                                ConsumerInstance.Seek(consumeResult.TopicPartitionOffset);
                            }
                        }
                    }
                }
                catch (TaskCanceledException) { }
                catch (KafkaException ex)
                {
                    if (ex.Message == "Broker: Group rebalance in progress")
                    {
                        _logger.LogWarning(ex, "KafkaHelper | Consumer: {GroupID},  Offset: {Offset}, Partition: {Partition}, Topic: {Topic}", ConsumerModel.GroupID, consumeResult?.TopicPartitionOffset?.Offset ?? -1, consumeResult?.Partition ?? -1, consumeResult?.Topic ?? "unknown");
                    }
                    else
                    {
                        _logger.LogError(ex, "KafkaHelper | Consumer: {GroupID},  Offset: {Offset}, Partition: {Partition}, Topic: {Topic}", ConsumerModel.GroupID, consumeResult?.TopicPartitionOffset?.Offset ?? -1, consumeResult?.Partition ?? -1, consumeResult?.Topic ?? "unknown");
                        ConsumerIsActive = false;
                        throw;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "KafkaHelper | Consumer: {GroupID},  Offset: {Offset}, Partition: {Partition}, Topic: {Topic}", ConsumerModel.GroupID, consumeResult?.TopicPartitionOffset?.Offset ?? -1, consumeResult?.Partition ?? -1, consumeResult?.Topic ?? "unknown");
                    ConsumerIsActive = false;
                    throw;
                }
            }
        }

        #region Pause_Functionality

        public void PauseConsumer(int pauseLengthInMilliseconds, bool commitPendingMessages, CancellationToken cancellationToken)
        {
            PauseConsumer(topicPartitions: null, topics: null, pauseLengthInMilliseconds, commitPendingMessages, cancellationToken);
        }

        public void PauseConsumer(string topicName, int pauseLengthInMilliseconds, bool commitPendingMessages, CancellationToken cancellationToken)
        {
            PauseConsumer(topicPartitions: null, topics: new List<string> { topicName }, pauseLengthInMilliseconds, commitPendingMessages, cancellationToken);
        }

        public void PauseConsumer(List<string> topicNames, int pauseLengthInMilliseconds, bool commitPendingMessages, CancellationToken cancellationToken)
        {
            PauseConsumer(topicPartitions: null, topics: topicNames, pauseLengthInMilliseconds, commitPendingMessages, cancellationToken);
        }

        public void PauseConsumer(List<TopicPartition> topicPartitions, int pauseLengthInMilliseconds, bool commitPendingMessages, CancellationToken cancellationToken)
        {
            PauseConsumer(topicPartitions: topicPartitions, topics: null, pauseLengthInMilliseconds, commitPendingMessages, cancellationToken);
        }

        private void PauseConsumer(List<TopicPartition> topicPartitions, List<string> topics, int pauseLengthInMilliseconds, bool commitPendingMessages, CancellationToken cancellationToken)
        {
            if (pauseLengthInMilliseconds < 1)
            {
                _logger.LogWarning("PauseLengthInMilliseconds must be more than 0. Consumer will not pause. PauseLengthInMilliseconds passed in: {pauseLengthInMilliseconds}", pauseLengthInMilliseconds);
                return;
            }

            var consumerTopicPartitions = ConsumerInstance.Assignment;
            var topicPartitionsToPause = new List<TopicPartition>();

            foreach (var ctp in consumerTopicPartitions)
            {
                var pauseThisCTP = false;
                if (topicPartitions != null && topicPartitions.Count > 0)
                {
                    //passed in topic partitions
                    if (topicPartitions.Any(tp => tp.Equals(ctp)))
                    {
                        pauseThisCTP = true;
                    }
                }
                else if (topics != null && topics.Count > 0)
                {
                    //passed in topic names
                    if (topics.Any(t => t == ctp.Topic))
                    {
                        pauseThisCTP = true;
                    }
                }
                else
                {
                    //pause all
                    pauseThisCTP = true;
                }
                if (pauseThisCTP)
                {
                    topicPartitionsToPause.Add(ctp);
                }
            }

            if (topicPartitionsToPause.Count > 0)
            {
                //be sure that we don't already have these topic partitions paused, and if so, remove them from the resume we have waiting
                if (pauseModels.Any())
                {
                    foreach (var c in pauseModels)
                    {
                        c.Value.TopicPartitions = c.Value.TopicPartitions.Where(tp => !topicPartitionsToPause.Any(tpp => tpp.Equals(tp))).ToList();
                    }
                }

                var pausedConsumer = new PauseModel
                {
                    CancellationTokenForResumeTask = cancellationToken,
                    TopicPartitions = topicPartitionsToPause,
                    ResumeGuid = Guid.NewGuid(),
                    CommitPendingMessages = commitPendingMessages
                };
                var resumeTask = Task.Delay(pauseLengthInMilliseconds, pausedConsumer.CancellationTokenForResumeTask).ContinueWith(task =>
                {
                    ResumePausedConsumer(pausedConsumer.ResumeGuid);
                }, cancellationToken);
                pausedConsumer.ResumeTask = resumeTask;
                pauseModels.TryAdd(pausedConsumer.ResumeGuid, pausedConsumer);
                ConsumerInstance.Pause(pausedConsumer.TopicPartitions);
            }   
        }

        private void ResumePausedConsumer(Guid resumeGuid)
        {
            if (pauseModels.TryRemove(resumeGuid, out var pauseModel))
            {
                if (pauseModel.TopicPartitions.Any())
                {
                    ConsumerInstance.Resume(pauseModel.TopicPartitions);
                }
            }
        }

        #endregion

        #region IDisposable_Implementation
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
                if (ConsumerInstance != null)
                {
                    ConsumerInstance.Dispose();
                    ConsumerInstance = null;
                }
        }
        #endregion
    }
}
