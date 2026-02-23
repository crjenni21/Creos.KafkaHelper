
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

        event ConsumeEventHandler ConsumeEvent;
    }

    public class ConsumerMember : IConsumerMember, IDisposable
    {
        private readonly ILogger<ConsumerMember> _logger;
        private readonly IKafkaHelperFunctions _helper;
        private readonly ConcurrentDictionary<Guid, PauseModel> pauseModels = new();
        internal readonly ConcurrentBag<TopicPartitionOffset> TopicPartitionOffsets_ToCommit;
        public IConsumer<string, string> ConsumerInstance { get; private set; }

        public int InstanceNumber { get; private set; }

        internal ConsumerMember(IServiceProvider serviceProvider, ConsumerModel consumerModel, int instanceNumber)
        {
            _logger = serviceProvider.GetRequiredService<ILogger<ConsumerMember>>();
            ConsumerModel = consumerModel;
            _helper = serviceProvider.GetRequiredService<IKafkaHelperFunctions>();
            TopicPartitionOffsets_ToCommit = new ConcurrentBag<TopicPartitionOffset>();
            ConsumerInstance = _helper.BuildConsumer(consumerModel);
            _logger.LogTrace("Consumer Name: {ConsumerName} Instantiated. InstanceNumber: {InstanceNumber}", ConsumerModel.ConsumerName, instanceNumber);
            InstanceNumber = instanceNumber;
        }

        public ConsumerModel ConsumerModel { get; private set; }
        public event ConsumeEventHandler ConsumeEvent;

        public bool ConsumerIsActive { get; private set; } = false;

        internal List<string> Topics { get; set; }

        internal DateTime DateTimeLastCommit { get; set; }

        public async Task RegisterConsumerMemberAsync(CancellationToken cancellationToken)
        {
            //var topics = new List<string>();
            var topics = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var raw in ConsumerModel.Topics)
            {
                var topic = raw?.Trim();
                if (string.IsNullOrWhiteSpace(topic)) continue;

                if (!topics.Contains(topic))
                {
                    if (topic.Contains('*') || topic.Contains('+'))
                    {
                        foreach (var t in await _helper.GetTopicsFromWildcardAsync(topic).ConfigureAwait(false))
                        {
                            topics.Add(t);
                        }
                    }
                    else
                        topics.Add(topic);
                }
            }

            _logger.LogDebug("KafkaHelper | Consumer Name: {Name}, InstanceNumber: {InstanceNumber}, Subscribing to {TopicList}", ConsumerModel.ConsumerName, InstanceNumber, string.Join(",", topics.Distinct()));

            Topics = topics.Distinct().ToList();

            var actualTopicListToSubscribe = new List<string>();
            foreach (var topic in Topics)
            {
                if (_helper.TopicList.Contains(topic, StringComparer.OrdinalIgnoreCase))
                    actualTopicListToSubscribe.Add(topic);
                else
                    _logger.LogWarning("KafkaHelper | Consumer Name: {Name} will not consume from {topic} because this topic does not exist.", ConsumerModel.ConsumerName, topic);
            }

            if (actualTopicListToSubscribe.Count == 0)
                return;

            if ((!string.IsNullOrWhiteSpace(ConsumerModel.ConsumeFromDate)) && DateTime.TryParse(ConsumerModel.ConsumeFromDate, out DateTime consumeFromDate))
            {
                var consumeFromDateForTimestamp = new DateTime(year: consumeFromDate.Year, month: consumeFromDate.Month, day: consumeFromDate.Day, hour: consumeFromDate.Hour, minute: consumeFromDate.Minute, second: consumeFromDate.Second, DateTimeKind.Utc);
                var consumeFromDateTimestamp = new Timestamp(consumeFromDateForTimestamp, TimestampType.CreateTime);

                var config = new AdminClientConfig { BootstrapServers = _helper.BrokerList };
                using var adminClient = new AdminClientBuilder(config).Build();

                var topicPartitionTimestamps = new List<TopicPartitionTimestamp>();
                var topicPartitionOffsets = new List<TopicPartitionOffset>();

                foreach (var topic in actualTopicListToSubscribe)
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
            ConsumerInstance.Subscribe(actualTopicListToSubscribe);

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

        public async Task ExecuteConsumingAsync(CancellationToken ct)
        {
            try
            {
                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                    ConsumeResult<string, string> consumeResult = null;
                    try
                    {
                        consumeResult = ConsumerInstance.Consume(ct);
                        if (!consumeResult.IsPartitionEOF)
                        {
                            if (!string.IsNullOrWhiteSpace(consumeResult?.Message?.Value)) // A null value could be a "tombstone record" and may should be handled differently. 
                            {
                                bool ok = await RaiseConsumeEventAsync(consumeResult, ct).ConfigureAwait(false);
                                if (!ok)
                                {
                                    throw new Exception("KafkaHelper | Consumer Failed. RaiseConsumeEventAsync returned false.");
                                }
                            }
                            if (!ConsumerModel.EnableAutoCommit)
                            {
                                var doNotCommit = pauseModels.Any(pc => !pc.Value.CommitPendingMessages && pc.Value.TopicPartitions.Any(tp => tp.Equals(consumeResult.TopicPartition)));
                                if (!doNotCommit)
                                {
                                    TopicPartitionOffsets_ToCommit.Add(consumeResult.TopicPartitionOffset);
                                }
                                else
                                {
                                    ConsumerInstance.Seek(consumeResult.TopicPartitionOffset);
                                }
                            }
                        }
                    }
                    catch (KafkaException ex)
                    {
                        if (ex.Message == "Broker: Group rebalance in progress")
                        {
                            _logger.LogWarning(ex, "KafkaHelper | Consumer: {GroupID},  Offset: {Offset}, Partition: {Partition}, Topic: {Topic}, InstanceNumber: {InstanceNumber}", ConsumerModel.GroupID, consumeResult?.TopicPartitionOffset?.Offset ?? -1, consumeResult?.Partition ?? -1, consumeResult?.Topic ?? "unknown", InstanceNumber);
                        }
                        else
                        {
                            _logger.LogError(ex, "KafkaHelper | Consumer: {GroupID},  Offset: {Offset}, Partition: {Partition}, Topic: {Topic}, InstanceNumber: {InstanceNumber}", ConsumerModel.GroupID, consumeResult?.TopicPartitionOffset?.Offset ?? -1, consumeResult?.Partition ?? -1, consumeResult?.Topic ?? "unknown", InstanceNumber);
                            ConsumerIsActive = false;
                            throw;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "KafkaHelper | Consumer: {GroupID},  Offset: {Offset}, Partition: {Partition}, Topic: {Topic}, InstanceNumber: {InstanceNumber}", ConsumerModel.GroupID, consumeResult?.TopicPartitionOffset?.Offset ?? -1, consumeResult?.Partition ?? -1, consumeResult?.Topic ?? "unknown", InstanceNumber);
                        ConsumerIsActive = false;
                        throw;
                    }
                }
            }
            catch (OperationCanceledException ex) when (ct.IsCancellationRequested)
            {
                _logger.LogWarning(ex, "");
            }

        }

        private async Task<bool> RaiseConsumeEventAsync(ConsumeResult<string, string> cr, CancellationToken ct)
        {
            var handler = ConsumeEvent;
            if (handler is null) return true;

            var args = new ConsumeTriggerEventArgs(cr, InstanceNumber, ct);

            foreach (ConsumeEventHandler h in handler.GetInvocationList())
            {
                ct.ThrowIfCancellationRequested();

                bool ok = await h(this, args).ConfigureAwait(false);
                if (!ok) return false;
            }

            return true;
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
                if (ConsumerInstance is not null)
                {
                    try { ConsumerInstance.Close(); } catch { }
                    ConsumerInstance.Dispose();
                    ConsumerInstance = null;
                }
        }
        #endregion
    }
}
