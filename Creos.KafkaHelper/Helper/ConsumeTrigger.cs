
using Confluent.Kafka;

namespace Creos.KafkaHelper.Helper
{

    public class ConsumeTriggerEventArgs : EventArgs
    {
        public ConsumeTriggerEventArgs(ConsumeResult<string, string> consumeResult)
        {
            ConsumeResult = consumeResult;
        }

        public ConsumeResult<string, string> ConsumeResult { get; set; }
    }

    public delegate Task<bool> AsyncEventHandler<TEventArgs>(ConsumeTriggerEventArgs consumeTriggerEvent);
}
