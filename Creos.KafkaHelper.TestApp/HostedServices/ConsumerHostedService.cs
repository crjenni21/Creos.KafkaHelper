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

        private int counter = 0;

        public ConsumerHostedService(ILogger<ConsumerHostedService> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _consumerMembers = serviceProvider.GetServices<ConsumerMember>().Where(x => x.ConsumerModel.Active && x.ConsumerModel.ConsumerName == "TestConsumer1");
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            if (_consumerMembers != null && _consumerMembers.Count() > 0)
            {
                try
                {
                    foreach (var consumerMember in _consumerMembers)
                    {
                        await consumerMember.RegisterConsumerMemberAsync(cancellationToken);
                        consumerMember.ConsumeEvent += ProcessConsumedMessageAsync;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError("Here");
                }
            }
        }

        protected override async Task<bool> ProcessConsumedMessageAsync(object sender, ConsumeTriggerEventArgs consumeTriggerEvent)
        {

            var consumeResult = consumeTriggerEvent.ConsumeResult;
            var i = consumeTriggerEvent.InstanceNumber;
            _logger.LogDebug("Topic: {Topic}, offset: {Offset}, Partition: {Partition}, InstanceNumber: {InstanceNumber}", consumeResult.Topic, consumeResult.Offset, consumeResult.Partition.Value, i);
            var x = await ProcessConsumedMessage_private(consumeResult, consumeResult.Partition.Value, consumeTriggerEvent.CancellationToken);

            if (!x)
            {
                ((ConsumerMember)sender).PauseConsumer(5000, false, consumeTriggerEvent.CancellationToken);
                return true;
            }
            return x;
        }

        private async Task<bool> ProcessConsumedMessage_private(ConsumeResult<string, string> ConsumeResult, int partition, CancellationToken cancellationToken)
        {
            counter++;

            if (counter > 5)
            {
                
                return false;
            }
            else
            {
                await Task.Delay(500, cancellationToken);
            }
                
            return true;
        }
    }
}
