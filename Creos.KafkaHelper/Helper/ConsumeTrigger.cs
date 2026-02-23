
using Confluent.Kafka;

namespace Creos.KafkaHelper.Helper
{

    public class ConsumeTriggerEventArgs : EventArgs
    {
        public ConsumeTriggerEventArgs(ConsumeResult<string, string> consumeResult, int instanceNumber, CancellationToken cancellationToken)
        {
            ConsumeResult = consumeResult;
            InstanceNumber = instanceNumber;
            CancellationToken = cancellationToken;
        }

        public ConsumeResult<string, string> ConsumeResult { get; }
        public int InstanceNumber { get; }
        public CancellationToken CancellationToken { get; }
    }

    public delegate Task<bool> ConsumeEventHandler(object sender, ConsumeTriggerEventArgs e);
}
