
using Creos.KafkaHelper.Consumer;
using Creos.KafkaHelper.Exceptions;
using Creos.KafkaHelper.Helper;
using Creos.KafkaHelper.HostedServices;
using Creos.KafkaHelper.Models;
using Creos.KafkaHelper.Producer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Creos.KafkaHelper.Extentions
{
    public static class ServiceCollectionExtension
    {
        public static IServiceCollection AddKafkaHelper(this IServiceCollection services, IConfiguration configuration)
        {
            if (services == null) throw new ArgumentNullException(nameof(services));

            var kafkaConfigModel = configuration.GetSection("KafkaConfiguration").Get<KafkaConfigurationModel>(options => options.BindNonPublicProperties = true);

            services.AddSingleton<IKafkaProducer, KafkaProducer>();
            services.AddTransient<IConsumerAccessor, ConsumerAccessor>();

            if (ValidateKafkaConfigurationModel(kafkaConfigModel))
            {
                services.AddSingleton(new ProducerMessages());

                services.AddSingleton<IKafkaHelperFunctions, KafkaHelperFunctions>();
                

                services.AddKafkaScopedConsumers(kafkaConfigModel);
                services.AddKafkaScopedProducers(kafkaConfigModel);
                if (kafkaConfigModel.Consumers.Where(x => !x.EnableAutoCommit).Any())
                {
                    services.AddHostedService<ComsumerCommitterService>();
                }
                if (kafkaConfigModel.Producers.Where(x => x.Active).Any())
                {
                    services.AddHostedService<ProducerLoggerService>();
                }

                services.AddHostedService<WildcardConsumerService>();
            }

            return services;
        }

        private static IServiceCollection AddKafkaScopedConsumers(this IServiceCollection services, KafkaConfigurationModel kafkaConfigModel)
        {
            if (kafkaConfigModel.Consumers?.Where(x => x.Active).Any() ?? false)
            {
                foreach (var consumerModel in kafkaConfigModel.Consumers?.Where(x => x.Active))
                {
                    services.AddSingleton(serviceProvider =>
                    {
                        return new ConsumerMember(serviceProvider, consumerModel);
                    });
                }
            }
            return services;
        }

        private static IServiceCollection AddKafkaScopedProducers(this IServiceCollection services, KafkaConfigurationModel kafkaConfigurationModel)
        {
            if (kafkaConfigurationModel.Producers?.Where(x => x.Active).Any() ?? false)
            {
                foreach (var producerModel in kafkaConfigurationModel?.Producers.Where(x => x.Active))
                {
                    services.AddTransient<IScopedProducer, ScopedProducer>(serviceProvider =>
                    {
                        return new ScopedProducer(serviceProvider, producerModel);
                    });
                }
            }

            return services;
        }

        private static bool ValidateKafkaConfigurationModel(KafkaConfigurationModel kafkaConfigurationModel)
        {
            if (kafkaConfigurationModel == null) return false; // appsettings not configured, so don't do any work. 

            if ((!kafkaConfigurationModel.Consumers?.Where(x => x.Active).Any() ?? true) && (!kafkaConfigurationModel?.Producers.Where(x => x.Active).Any() ?? true))
            {
                // no active producers or consumers, so don't do any work.
                return false;
            }

            if (kafkaConfigurationModel.Producers?.Where(x => x.Active && string.IsNullOrWhiteSpace(x.ProducerName)).Any() ?? false)
            {
                throw new ConfigurationMissingException("KafkaHelper | Producers:ProducerName must be defined on each active Producer.");
            }

            if (kafkaConfigurationModel.Producers?.Where(x => x.Active).GroupBy(x => x.ProducerName).Where(x => x.Skip(1).Any()).Any() ?? false)
            {
                throw new ConfigurationMissingException("KafkaHelper | Producers:ProducerName must be unique for each active Producer.");
            }

            if (kafkaConfigurationModel.Brokers == null || kafkaConfigurationModel.Brokers.Count == 0)
            {
                throw new KafkaBrokerMissingException("KafkaHelper | KafkaConfiguration:Brokers does not contain valid Bootstrap Server Connections");
            }

            foreach (var broker in kafkaConfigurationModel.Brokers)
            {
                if (broker.Split(":").Length != 2)
                {
                    throw new KafkaBrokerInvalidException($"KafkaHelper | KafkaConfiguration:Brokers: {broker} -- Invalid port or poorly formatted");
                }
            }

            return true;
        }

    }
}
