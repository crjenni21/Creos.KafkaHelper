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
        private readonly IEnumerable<ConsumerMember> _consumerMembers;
        private CancellationToken _token;

        public ConsumerHostedService(ILogger<ConsumerHostedService> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _consumerMembers = serviceProvider.GetServices<ConsumerMember>().Where(x => x.ConsumerModel.Active && x.ConsumerModel.ConsumerName == "TestConsumer1");
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _token = cancellationToken;
            if (_consumerMembers != null && _consumerMembers.Count() > 0)
            {
                foreach (var consumerMember in _consumerMembers) 
                {
                    await consumerMember.RegisterConsumerMemberAsync(cancellationToken);
                    consumerMember.ConsumeEvent += ProcessConsumedMessageAsync;
                }
            }
        }

        protected override async Task<bool> ProcessConsumedMessageAsync(ConsumeTriggerEventArgs consumeTriggerEvent)
        {
            var consumeResult = consumeTriggerEvent.ConsumeResult;
            var i = consumeTriggerEvent.InstanceNumber;
            _logger.LogDebug("Topic: {Topic}, offset: {Offset}, Partition: {Partition}, InstanceNumber: {InstanceNumber}", consumeResult.Topic, consumeResult.Offset, consumeResult.Partition.Value, i);
            var x = await ProcessConsumedMessage_private(consumeResult, consumeResult.Partition.Value, _token);
            return x;
        }

        private async Task<bool> ProcessConsumedMessage_private(ConsumeResult<string, string> ConsumeResult, int partition, CancellationToken cancellationToken)
        {
            if (partition % 2 == 0)
                await Task.Delay(1000, cancellationToken);
            else
                await Task.Delay(8000, cancellationToken);
            return true;
        }
    }
}
