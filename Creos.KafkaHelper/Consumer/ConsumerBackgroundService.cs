using Confluent.Kafka;
using Creos.KafkaHelper.Helper;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Creos.KafkaHelper.Consumer
{
    public abstract class ConsumerBackgroundService : BackgroundService
    {
        protected abstract Task<bool> ProcessConsumedMessageAsync(ConsumeTriggerEventArgs consumeTriggerEvent);
    }
}
