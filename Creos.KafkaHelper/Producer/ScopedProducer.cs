
using Confluent.Kafka;
using Creos.KafkaHelper.Helper;
using Creos.KafkaHelper.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Creos.KafkaHelper.Producer
{
    internal interface IScopedProducer
    {
        void ProduceMessage(string TopicName, Message<string, string> message);
        Task ProduceMessageAsync(string topicName, Message<string, string> message, CancellationToken cancellationToken);
        string ProducerName { get; set; }
        string TopicName { get; set; }
        ProducerModel ProducerModel { get; set; }
    }
    internal class ScopedProducer : IScopedProducer
    {
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<ScopedProducer> _logger;

        public ScopedProducer(IServiceProvider serviceProvider, ProducerModel producerModel)
        {
            _logger = serviceProvider.GetRequiredService<ILogger<ScopedProducer>>();

            var kafkaHelperFunctions = serviceProvider.GetRequiredService<IKafkaHelperFunctions>();

            var confluentProducerConfig = new ProducerConfig
            {
                Acks = Acks.Leader,
                BootstrapServers = kafkaHelperFunctions.BuildBrokerList(producerModel),
                Partitioner = producerModel.Partitioner,
                BatchSize = producerModel.BatchSizeBytes,
                AllowAutoCreateTopics = producerModel.AllowAutoCreateTopics
            };

            if (producerModel.LingerMS > 0)
            {
                confluentProducerConfig.LingerMs = producerModel.LingerMS;
            }
            if (producerModel.MessageTimeoutMs > 0)
            {
                confluentProducerConfig.MessageTimeoutMs = producerModel.MessageTimeoutMs;
            }
            if (producerModel.MessageSendMaxRetries > 0)
            {
                confluentProducerConfig.MessageSendMaxRetries = producerModel.MessageSendMaxRetries;
            }
            if (producerModel.RequestTimeoutMs > 0)
            {
                confluentProducerConfig.RequestTimeoutMs = producerModel.RequestTimeoutMs;
            }
            if (producerModel.SocketTimeoutMs > 0)
            {
                confluentProducerConfig.SocketTimeoutMs = producerModel.SocketTimeoutMs;
            }
            if (producerModel.SocketConnectionSetupTimeoutMs > 0)
            {
                confluentProducerConfig.SocketConnectionSetupTimeoutMs = producerModel.SocketConnectionSetupTimeoutMs;
            }
            if (producerModel.RetryBackoffMaxMs > 0)
            {
                confluentProducerConfig.RetryBackoffMaxMs = producerModel.RetryBackoffMaxMs;
            }
            if (producerModel.ReconnectBackoffMaxMs > 0)
            {
                confluentProducerConfig.ReconnectBackoffMaxMs = producerModel.ReconnectBackoffMaxMs;
            }

            _producer = new ProducerBuilder<string, string>(confluentProducerConfig)
                .SetErrorHandler((producer, error) =>
                {
                    var level = error.IsFatal ? LogLevel.Critical : LogLevel.Warning;
                    _logger.Log(level, "KafkaHelper | ScopedProducer | Error: {Error}, Reason: {Reason}, IsFatal: {IsFatal}", error.Code, error.Reason, error.IsFatal);
                })
                .SetLogHandler((producer, logMessage) =>
                {
                    _logger.LogTrace("KafkaHelper | ScopedProducer | Log: {LogMessage}", logMessage.Message);
                })
                .Build();

            ProducerModel = producerModel;
            ProducerName = producerModel.ProducerName;
            TopicName = producerModel.Topic;
        }

        public string ProducerName { get; set; }
        public string TopicName { get; set; }
        public ProducerModel ProducerModel { get; set; }

        public void ProduceMessage(string TopicName, Message<string, string> message)
        {
            _producer.Produce(TopicName, message, report =>
            {
                if (report.Error.IsError)
                {
                    _logger.LogError("KafkaHelper | ScopedProducer | ProduceMessage | Error producing message to topic {TopicName}: {Error}, Reason: {Reason}, IsFatal: {IsFatal}", TopicName, report.Error.Code, report.Error.Reason, report.Error.IsFatal);
                }
                else
                {
                    _logger.LogInformation("KafkaHelper | ScopedProducer | ProduceMessage | Successfully produced message to topic {TopicName} at offset {Offset}", TopicName, report.Offset);
                }
            });
        }

        public async Task ProduceMessageAsync(string topicName, Message<string, string> message, CancellationToken cancellationToken)
        {
            var timeSpan = TimeSpan.FromSeconds(3);
            using var timeoutCts = new CancellationTokenSource(timeSpan);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancellationToken);
            try
            {
                var result = await _producer.ProduceAsync(topicName, message, linkedCts.Token).WaitAsync(linkedCts.Token);
                _logger.LogTrace("KafkaHelper | ScopedProducer | ProduceMessage | Successfully produced message to topic {TopicName} to TPO {TPO}", result.Topic, result.TopicPartitionOffset);
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
            {
                _logger.LogError("Kafka produce timed out after {TimeOut} seconds for topic {Topic}", timeSpan.Seconds, topicName);
                throw new TimeoutException($"Kafka produce timed out after 3 seconds for topic {topicName}");
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Kafka produce canceled by caller for topic {Topic}", topicName);
                throw;
            }
            catch (ProduceException<string, string> ex)
            {
                _logger.LogError(ex, "Kafka delivery failed for topic {Topic}. Reason: {Reason}", topicName, ex.Error.Reason);
                throw;
            }
        }
    }
}
