
using Creos.KafkaHelper.Producer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Creos.KafkaHelper.TestApp.HostedServices
{
    public class ProducerExampleService : BackgroundService
    {
        private readonly ILogger<ProducerExampleService> _logger;
        private readonly IKafkaProducer _kafkaProducer;
        public ProducerExampleService(ILogger<ProducerExampleService> logger, IKafkaProducer kafkaProducer)
        {
            _logger = logger;
            _kafkaProducer = kafkaProducer;
        }

        protected async override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            int i = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                Random random = new();
                var rand = random.Next(0, 100);
                if (i % 100 == 0)
                {
                    _logger.LogDebug("ProducerExampleService.ExecuteAsync Iteration: {i} RandomNumber: {RandomNumber}", i, rand);
                }
                
                await _kafkaProducer.ProduceMessageToKafkaAsync("Testing", new Confluent.Kafka.Message<string, string>() { Key = rand.ToString(), Value = rand.ToString() });
                await Task.Delay(10, cancellationToken);
                i++;
            }
        }
    }
}
