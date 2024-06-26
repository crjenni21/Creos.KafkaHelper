
using Confluent.Kafka;
using Creos.KafkaHelper.Exceptions;
using Creos.KafkaHelper.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;

namespace Creos.KafkaHelper.Helper
{
    internal interface IKafkaHelperFunctions
    {
        List<string> TopicList { get; }
        string BrokerList { get; }
        Task<List<string>> GetTopicsFromWildcardAsync(string wildcardName);
        string BuildBrokerList(ProducerModel producerModel);
        IConsumer<string, string> BuildConsumer(ConsumerModel consumerModel);
    }

    internal sealed class KafkaHelperFunctions : IKafkaHelperFunctions
    {
        private readonly ILogger<KafkaHelperFunctions> _logger;
        private readonly IConfiguration _configuration;
        public KafkaHelperFunctions(ILogger<KafkaHelperFunctions> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        private List<string> _topicList = null;
        public List<string> TopicList
        {
            get
            {
                _topicList ??= GetTopicsInKafka();
                return _topicList;
            }
        }

        private string _brokerList = null;
        public string BrokerList
        {
            get
            {
                if (string.IsNullOrWhiteSpace(_brokerList))
                {
                    List<string> brokerList = _configuration.GetSection("KafkaConfiguration").Get<KafkaConfigurationModel>()?.Brokers;
                    if (brokerList == null || brokerList.Count == 0)
                    {
                        throw new KafkaBrokerMissingException("KafkaHelper | KafkaConfiguration:Brokers does not contain valid Broker Connections");
                    }
                    _brokerList = string.Join(',', brokerList);
                }
                return _brokerList;
            }
        }

        private List<string> GetTopicsInKafka()
        {
            
            var config = new AdminClientConfig { BootstrapServers = BrokerList };
            using var adminClient = new AdminClientBuilder(config).Build();
            var topics = new List<string>();

            var metaData = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            foreach (var topicMetaData in metaData.Topics)
            {
                topics.Add(topicMetaData.Topic);
            }
            return topics;
        }

        public async Task<List<string>> GetTopicsFromWildcardAsync(string wildcardName)
        {
            await Task.Yield();
            _topicList = null;
            List<string> MatchingTopicList = new List<string>();
            foreach (var topic in TopicList)
            {
                if (!string.IsNullOrWhiteSpace(topic))
                {
                    MatchCollection mc = Regex.Matches(topic, wildcardName);
                    if (mc.Count > 0)
                        MatchingTopicList.Add(topic);
                }
            }

            return MatchingTopicList.Distinct().ToList();
        }
        
        private string BuildBrokerList(ConsumerModel consumerModel)
        {
            if (consumerModel.Brokers != null && consumerModel.Brokers.Count > 0)
                return string.Join(',', consumerModel.Brokers);
            else
                return BrokerList;
        }
        public string BuildBrokerList(ProducerModel producerModel)
        {
            if (producerModel.Brokers != null && producerModel.Brokers.Count > 0)
                return string.Join(',', producerModel.Brokers);
            else
                return BrokerList;
        }

        public IConsumer<string, string> BuildConsumer(ConsumerModel consumerModel)
        {
            ConsumerConfig confluentConsumerConfig = new()
            {
                BootstrapServers = BuildBrokerList(consumerModel),
                GroupId = consumerModel.GroupID,
                EnableAutoCommit = consumerModel.EnableAutoCommit,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | Broker(s): {BrokerList}", BrokerList);
            _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | GroupId: {confluentConsumerConfig.GroupId}", confluentConsumerConfig.GroupId);
            _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | EnableAutoCommit: {confluentConsumerConfig.EnableAutoCommit}", confluentConsumerConfig.EnableAutoCommit);
            _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | AutoOffsetReset: {confluentConsumerConfig.AutoOffsetReset}", confluentConsumerConfig.AutoOffsetReset);

            if (consumerModel.StatisticsIntervalMs != null)
            {
                confluentConsumerConfig.StatisticsIntervalMs = consumerModel.StatisticsIntervalMs;
                _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | StatisticsIntervalMs: {confluentConsumerConfig.StatisticsIntervalMs}", confluentConsumerConfig.StatisticsIntervalMs);
            }
            if (consumerModel.SessionTimeoutMs != null)
            {
                confluentConsumerConfig.SessionTimeoutMs = consumerModel.SessionTimeoutMs;
                _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | SessionTimeoutMs: {confluentConsumerConfig.SessionTimeoutMs}", confluentConsumerConfig.SessionTimeoutMs);
            }
            if (consumerModel.EnablePartitionEof != null)
            {
                confluentConsumerConfig.EnablePartitionEof = consumerModel.EnablePartitionEof;
                _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | EnablePartitionEof: {confluentConsumerConfig.EnablePartitionEof}", confluentConsumerConfig.EnablePartitionEof);
            }
            if (consumerModel.ReconnectBackoffMs != null)
            {
                confluentConsumerConfig.ReconnectBackoffMs = consumerModel.ReconnectBackoffMs;
                _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | ReconnectBackoffMs: {confluentConsumerConfig.ReconnectBackoffMs}", confluentConsumerConfig.ReconnectBackoffMs);
            }
            if (consumerModel.HeartbeatIntervalMs != null)
            {
                confluentConsumerConfig.HeartbeatIntervalMs = consumerModel.HeartbeatIntervalMs;
                _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | HeartbeatIntervalMs: {confluentConsumerConfig.HeartbeatIntervalMs}", confluentConsumerConfig.HeartbeatIntervalMs);
            }
            if (consumerModel.ReconnectBackoffMaxMs != null)
            {
                confluentConsumerConfig.ReconnectBackoffMaxMs = consumerModel.ReconnectBackoffMaxMs;
                _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | ReconnectBackoffMaxMs: {confluentConsumerConfig.ReconnectBackoffMaxMs}", confluentConsumerConfig.ReconnectBackoffMaxMs);
            }
            if (consumerModel.FetchMaxBytes != null)
            {
                confluentConsumerConfig.FetchMaxBytes = consumerModel.FetchMaxBytes;
                _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | FetchMaxBytes: {confluentConsumerConfig.FetchMaxBytes}", confluentConsumerConfig.FetchMaxBytes);
            }
            if (consumerModel.AutoCommitIntervalMs != null)
            {
                confluentConsumerConfig.AutoCommitIntervalMs = consumerModel.AutoCommitIntervalMs;
                _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | AutoCommitIntervalMs: {confluentConsumerConfig.AutoCommitIntervalMs}", confluentConsumerConfig.AutoCommitIntervalMs);
            }
            if (consumerModel.QueuedMaxMessagesKbytes != null)
            {
                confluentConsumerConfig.QueuedMaxMessagesKbytes = consumerModel.QueuedMaxMessagesKbytes;
                _logger.LogTrace("KafkaHelper | KafkaHelperFunctions | BuildConsumer | QueuedMaxMessagesKbytes: {confluentConsumerConfig.QueuedMaxMessagesKbytes}", confluentConsumerConfig.QueuedMaxMessagesKbytes);
            }

            return new ConsumerBuilder<string, string>(confluentConsumerConfig)
                .SetErrorHandler((consumer, error) =>
                {
                    _logger.LogError("KafkaHelper | Consumer.BuildConsumer error: {error.Reason}", error.Reason);
                })
                .SetLogHandler((consumer, logMessage) =>
                {
                    _logger.LogTrace("KafkaHelper |  Consumer.BuildConsumer message: {logMessage.Message}", logMessage.Message);
                }).Build();
        }
    }
}
