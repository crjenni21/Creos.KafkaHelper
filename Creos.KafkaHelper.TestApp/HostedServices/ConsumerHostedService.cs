using Confluent.Kafka;
using Creos.KafkaHelper.Consumer;
using Creos.KafkaHelper.Helper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Creos.KafkaHelper.TestApp.HostedServices
{
    public class ConsumerHostedService : ConsumerBackgroundService
    {
        private readonly ILogger<ConsumerHostedService> _logger;
        private readonly IConsumerMember _consumerMember;
        private CancellationToken _token;

        public ConsumerHostedService(ILogger<ConsumerHostedService> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _consumerMember = serviceProvider.GetServices<IConsumerMember>().Where(x => x.ConsumerModel.Active && x.ConsumerModel.ConsumerName == "TestConsumer").FirstOrDefault();
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _token = cancellationToken;
            if (_consumerMember != null)
            {
                await _consumerMember.RegisterConsumerMemberAsync(cancellationToken);
                _consumerMember.ConsumeEvent += ProcessConsumedMessageAsync;
            }
        }

        protected override async Task<bool> ProcessConsumedMessageAsync(ConsumeTriggerEventArgs consumeTriggerEvent)
        {
            var consumeResult = consumeTriggerEvent.ConsumeResult;
            _logger.LogDebug("Topic: {Topic}, offset: {Offset}, TopicPartitionOffset: {TopicPartitionOffset}", consumeResult.Topic, consumeResult.Offset, consumeResult.TopicPartitionOffset);
            var x = await ProcessConsumedMessage_private(consumeResult, _token);
            return x;
        }

        private async Task<bool> ProcessConsumedMessage_private(ConsumeResult<string, string> ConsumeResult, CancellationToken cancellationToken)
        {
            await Task.Delay(500);
            return true;
        }
    }
}
