
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
        Task ProduceMessageAsync(string TopicName, Message<string, string> message);
        string ProducerName { get; set; }
        string TopicName { get; set; }
        ProducerModel ProducerModel { get; set; }
    }
    internal class ScopedProducer : IScopedProducer
    {
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<ScopedProducer> _logger;
        private readonly ProducerMessages _producerMessages;

        public ScopedProducer(IServiceProvider serviceProvider, ProducerModel producerModel)
        {
            _logger = serviceProvider.GetRequiredService<ILogger<ScopedProducer>>();
            _producerMessages = serviceProvider.GetRequiredService<ProducerMessages>();

            var kafkaHelperFunctions = serviceProvider.GetRequiredService<IKafkaHelperFunctions>();

            var confluentProducerConfig = new ProducerConfig
            {
                BootstrapServers = kafkaHelperFunctions.BuildBrokerList(producerModel),
                Partitioner = producerModel.Partitioner,
                LingerMs = producerModel.LingerMS,
                BatchSize = producerModel.BatchSizeBytes,
                AllowAutoCreateTopics = producerModel.AllowAutoCreateTopics
            };
            _producer = new ProducerBuilder<string, string>(confluentProducerConfig).Build();

            ProducerModel = producerModel;
            ProducerName = producerModel.ProducerName;
            TopicName = producerModel.Topic;
        }

        public string ProducerName { get; set; }
        public string TopicName { get; set; }
        public ProducerModel ProducerModel { get; set; }

        public void ProduceMessage(string TopicName, Message<string, string> message)
        {
            _producer.Produce(TopicName, message);
        }

        public async Task ProduceMessageAsync(string TopicName, Message<string, string> message)
        {
            await Task.Yield();
            _producerMessages.ProducerTasks.Add(_producer.ProduceAsync(TopicName, message));
        }
    }
}
