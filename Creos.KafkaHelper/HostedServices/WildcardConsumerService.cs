
using Creos.KafkaHelper.Consumer;
using Creos.KafkaHelper.Helper;
using Creos.KafkaHelper.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Creos.KafkaHelper.HostedServices
{
    internal class WildcardConsumerService : BackgroundService
    {
        private readonly ILogger<WildcardConsumerService> _logger;
        private IEnumerable<ConsumerMember> _consumerMembers;
        private readonly IKafkaHelperFunctions _kafkaHelperFunctions;
        private readonly KafkaConfigurationModel _kafkaConfigurationModel;

        public WildcardConsumerService(ILogger<WildcardConsumerService> logger, IServiceProvider serviceProvider, IKafkaHelperFunctions kafkaHelperFunctions, IConfiguration configuration)
        {
            _logger = logger;
            _consumerMembers = serviceProvider.GetServices<ConsumerMember>().Where(x => x.ConsumerModel.Active);
            _kafkaHelperFunctions = kafkaHelperFunctions;
            _kafkaConfigurationModel = configuration.GetSection("KafkaConfiguration").Get<KafkaConfigurationModel>();
        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Delay(15000, stoppingToken);
            _consumerMembers = _consumerMembers.Where(x => x.ConsumerModel.Topics.Any(x => x.Contains('*')));

            if (!_consumerMembers.Any())
            {
                return;
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_kafkaConfigurationModel.WildCardRestartTimer_Minutes * 60 * 1000, stoppingToken);

                    foreach (var member in _consumerMembers)
                    {
                        var newTopicList = new List<string>();
                        foreach (var topic in member.ConsumerModel.Topics)
                        {
                            newTopicList.AddRange(await _kafkaHelperFunctions.GetTopicsFromWildcardAsync(topic));
                        }

                        var newTopicFound = false;
                        foreach (var topic in newTopicList)
                        {
                            if (member.Topics.Contains(topic, StringComparer.OrdinalIgnoreCase))
                                continue;
                            else
                            {
                                _logger.LogWarning("KafkaHelper | WildcardConsumerService has found a new Topic {NewTopic}", topic);
                                newTopicFound = true;
                                break;
                            }
                        }

                        if (newTopicFound)
                        {
                            member.ConsumerInstance.Unsubscribe();
                            await Task.Delay(60000, stoppingToken); // 1 minute
                            await member.RegisterConsumerMemberAsync(stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "KafkaHelper | WildcardConsumerService");
                }
            }
        }
    }
}
