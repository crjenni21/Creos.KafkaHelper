using Confluent.Kafka;
using Creos.KafkaHelper.Consumer;
using Creos.KafkaHelper.Helper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Creos.KafkaHelper.TestApp.HostedServices
{
    public class ConsumerHostedService2 : ConsumerBackgroundService
    {
        private readonly ILogger<ConsumerHostedService2> _logger;
        private readonly ConsumerMember _consumerMember;
        public ConsumerHostedService2(ILogger<ConsumerHostedService2> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _consumerMember = serviceProvider.GetServices<ConsumerMember>().Where(x => x.ConsumerModel.Active && x.ConsumerModel.ConsumerName == "ServiceDependencyHealth").FirstOrDefault();
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            if (_consumerMember != null)
            {
                await _consumerMember.RegisterConsumerMemberAsync(cancellationToken);
                _consumerMember.ConsumeEvent += ProcessConsumedMessageAsync;
            }
        }

        protected override async Task<bool> ProcessConsumedMessageAsync(ConsumeTriggerEventArgs consumeTriggerEvent)
        {
            var consumeResult = consumeTriggerEvent.ConsumeResult;
            _logger.LogDebug("ServiceDependencyHealth | Topic: {Topic}, offset: {Offset}, TopicPartitionOffset: {Offset}", consumeResult.Topic, consumeResult.Offset, consumeResult.TopicPartitionOffset.Offset);
            return await ProcessConsumedMessage_private(consumeResult);
        }

        private async Task<bool> ProcessConsumedMessage_private(ConsumeResult<string, string> ConsumeResult)
        {
            await Task.Delay(10000);
            return true;
        }
    }
}
