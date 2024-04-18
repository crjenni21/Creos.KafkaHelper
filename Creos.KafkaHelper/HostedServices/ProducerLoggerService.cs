
using Creos.KafkaHelper.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Creos.KafkaHelper.HostedServices
{
    internal class ProducerLoggerService : BackgroundService
    {
        private readonly ILogger<ProducerLoggerService> _logger;
        private readonly ProducerMessages _producerMessages;
        private readonly IConfiguration _configuration;

        public ProducerLoggerService(ILogger<ProducerLoggerService> logger, IServiceProvider serviceProvider, IConfiguration configuration)
        {
            _logger = logger;
            _producerMessages = serviceProvider.GetRequiredService<ProducerMessages>();
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            
            try
            {
                var kafkaConfigModel = _configuration.GetSection("KafkaConfiguration").Get<KafkaConfigurationModel>();

                if (kafkaConfigModel.Producers.Where(x => x.Active).Any())
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            await Task.Delay(100, cancellationToken);
                            if (_producerMessages.ProducerTasks.Where(x => !x.IsCompleted).Any())
                            {
                                await Task.WhenAll(_producerMessages.ProducerTasks).ConfigureAwait(false);
                                _producerMessages.ProducerTasks.Clear();
                                _logger.LogTrace("KafkaHelper | After Lingering, sent Messages to Kafka");
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "KafkaHelper | ProducerLoggerService ExecuteAsync");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KafkaHelper | ProducerLoggerService ExecuteAsync");
            }
        }
    }
}
