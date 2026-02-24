
using Confluent.Kafka;
using Creos.KafkaHelper.Exceptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Creos.KafkaHelper.Producer
{
    public interface IKafkaProducer
    {
        /// <summary>
        /// Asynchronusly produce a message to a defined topic.  Configuration for this produced message will respect the settings of the passed in producerName.  However, the topic defined in the configuration is ignored.
        /// </summary>
        /// <param name="producerName"></param>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        /// <exception cref="KafkaProducerNameNotFoundException"></exception>
        Task ProduceMessageToKafkaAsync(string producerName, string topic, Message<string, string> message, CancellationToken cancellationToken = default);

        /// <summary>
        /// Synchronusly produce a message to a defined topic.  Configuration for this produced message will respect the settings of the passed in producerName.  However, the topic defined in the configuration is ignored.
        /// </summary>
        /// <param name="producerName"></param>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        /// <exception cref="KafkaProducerNameNotFoundException"></exception>
        void ProduceMessageToKafka(string producerName, string topic, Message<string, string> message);

        /// <summary>
        /// Asynchronusly produce a message to a specific topic.  Configuration for this produced message will respect the settings of the passed in producerName.  The topic produced to is defined in the configuration.
        /// </summary>
        /// <param name="producerName"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        /// <exception cref="KafkaProducerNameNotFoundException"></exception>
        Task ProduceMessageToKafkaAsync(string producerName, Message<string, string> message, CancellationToken cancellationToken = default);

        /// <summary>
        /// Synchronusly produce a message to a specific topic.  Configuration for this produced message will respect the settings of the passed in producerName.  The topic produced to is defined in the configuration.
        /// </summary>
        /// <param name="producerName"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        /// <exception cref="KafkaProducerNameNotFoundException"></exception>
        void ProduceMessageToKafka(string producerName, Message<string, string> message);
    }

    public sealed class KafkaProducer : IKafkaProducer
    {
        private readonly ILogger<KafkaProducer> _logger;
        private readonly IEnumerable<IScopedProducer> _scopedProducers;
        
        public KafkaProducer(ILogger<KafkaProducer> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _scopedProducers = serviceProvider.GetServices<IScopedProducer>();
        }

        private void Validate(string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                _logger.LogError("KafkaHelper | Topic name cannot be blank.");
                throw new KafkaProducerNameNotFoundException("KafkaHelper | Topic Name cannot be blank.");
            }
            // Considered putting a regex here to ensure that the Topic name is allowed.  However, this could potentially slow down the producing of messages and would 
            // be redundant.    A check already exists within Confluent.Kafka library to ensure that Topic Names are valid. 
        }

       
        public async Task ProduceMessageToKafkaAsync(string producerName, string topic, Message<string, string> message, CancellationToken cancellationToken = default)
        {
            try
            {
                Validate(topic);

                var scopedProducer = _scopedProducers.Where(x => string.Equals(x.ProducerName.Trim(), producerName.Trim(), StringComparison.OrdinalIgnoreCase))?.FirstOrDefault();
                if (scopedProducer != null)
                {
                    if (scopedProducer.ProducerModel.Active)
                    {
                        _logger.LogTrace("KafkaHelper | KafkaProduer.ProduceMessageToKafkaAsync Topic: {Topic}, Key: {Key}", topic, message.Key);
                        await scopedProducer.ProduceMessageAsync(topic, message, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        _logger.LogWarning("KafkaHelper | KafkaProducer.ProduceMessageToKafkaAsync ProdcuerName: {ProdcuerName} is not active.  No message produced.", producerName);
                    }
                }
                else
                {
                    throw new KafkaProducerNameNotFoundException($"KafkaHelper | KafkaProduer.ProduceMessageToKafkaAsync ProducerName {producerName} not found.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KafkaHelper | KafkaProduer.ProduceMessageToKafkaAsync ProducerName: {ProducerName} Topic: {Topic}, Key: {Key}", producerName, topic, message);
                throw;
            }
        }

        public void ProduceMessageToKafka(string producerName, string topic, Message<string, string> message)
        {
            try
            {
                Validate(topic);

                var scopedProducer = _scopedProducers.Where(x => string.Equals(x.ProducerName.Trim(), producerName.Trim(), StringComparison.OrdinalIgnoreCase))?.FirstOrDefault();
                if (scopedProducer != null)
                {
                    if (scopedProducer.ProducerModel.Active)
                    {
                        _logger.LogTrace("KafkaHelper | KafkaProduer.ProduceMessageToKafka Topic: {Topic}, Key: {Key}", topic, message.Key);
                        scopedProducer.ProduceMessage(topic, message);
                    }
                    else
                    {
                        _logger.LogWarning("KafkaHelper | KafkaProducer.ProduceMessageToKafka ProdcuerName: {ProdcuerName} is not active.  No message produced.", producerName);
                    }
                }
                else
                {
                    throw new KafkaProducerNameNotFoundException($"KafkaHelper | KafkaProduer.ProduceMessageToKafka ProducerName {producerName} not found.");
                }


            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KafkaHelper | KafkaProduer.ProduceMessageToKafka ProducerName: {ProducerName} Topic: {Topic}, Key: {Key}", producerName, topic, message.Key);
                throw;
            }
        }

        
        public async Task ProduceMessageToKafkaAsync(string producerName, Message<string, string> message, CancellationToken cancellationToken = default)
        {
            try
            {
                var scopedProducer = _scopedProducers.Where(x => string.Equals(x.ProducerName.Trim(), producerName.Trim(), StringComparison.OrdinalIgnoreCase))?.FirstOrDefault();
                if (scopedProducer != null)
                {
                    if (scopedProducer.ProducerModel.Active)
                    {
                        Validate(scopedProducer.TopicName);
                        _logger.LogTrace("KafkaHelper | KafkaProduer.ProduceMessageToKafkaAsync Topic: {Topic}, Key: {Key}", scopedProducer.TopicName, message.Key);
                        await scopedProducer.ProduceMessageAsync(scopedProducer.TopicName, message, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        _logger.LogWarning("KafkaHelper | KafkaProducer.ProduceMessageToKafkaAsync ProdcuerName: {ProdcuerName} is not active.  No message produced.", producerName);
                    }
                    
                }
                else
                {
                    throw new KafkaProducerNameNotFoundException($"KafkaHelper | KafkaProduer.ProduceMessageToKafkaAsync ProducerName {producerName} not found.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KafkaHelper | KafkaProduer ProduceMessageToKafkaAsync | ProducerName: {ProducerName}, Key: {Key}", producerName, message.Key);
                throw;
            }
        }

        /// <summary>
        /// Synchronously produce a message to a specific 
        /// </summary>
        /// <param name="producerName"></param>
        /// <param name="message"></param>
        /// <exception cref="KafkaProducerNameNotFoundException"></exception>
        public void ProduceMessageToKafka(string producerName, Message<string, string> message)
        {
            try
            {
                var scopedProducer = _scopedProducers.Where(x => string.Equals(x.ProducerName.Trim(), producerName.Trim(), StringComparison.OrdinalIgnoreCase))?.FirstOrDefault();
                if (scopedProducer != null)
                {
                    if (scopedProducer.ProducerModel.Active)
                    {
                        Validate(scopedProducer.TopicName);
                        _logger.LogTrace("KafkaHelper | KafkaProduer.ProduceMessageToKafka Topic: {Topic}, Key: {Key}", scopedProducer.TopicName, message.Key);
                        scopedProducer.ProduceMessage(scopedProducer.TopicName, message);
                    }
                    else
                    {
                        _logger.LogWarning("KafkaHelper | KafkaProducer.ProduceMessageToKafka ProdcuerName: {ProdcuerName} is not active.  No message produced.", producerName);
                    }
                }
                else
                {
                    throw new KafkaProducerNameNotFoundException($"KafkaHelper | KafkaProduer.ProduceMessageToKafka ProducerName {producerName} not found.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "KafkaHelper | KafkaProduer.ProduceMessageToKafka ProducerName: {ProducerName}, Key: {Key}", producerName, message.Key);
                throw;
            }
        }

    }
}
