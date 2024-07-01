
using Confluent.Kafka;

namespace Creos.KafkaHelper.Helper
{

    public class ConsumeTriggerEventArgs : EventArgs
    {
        public ConsumeTriggerEventArgs(ConsumeResult<string, string> consumeResult, int instanceNumber)
        {
            ConsumeResult = consumeResult;
            InstanceNumber = instanceNumber;
        }

        public ConsumeResult<string, string> ConsumeResult { get; set; }
        public int InstanceNumber { get; set; }
    }

    public delegate Task<bool> AsyncEventHandler<TEventArgs>(ConsumeTriggerEventArgs consumeTriggerEvent);
}
