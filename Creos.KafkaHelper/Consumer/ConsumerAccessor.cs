
using Creos.KafkaHelper.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Creos.KafkaHelper.Consumer
{
    public interface IConsumerAccessor
    {

        /// <summary>
        /// Determines if any active consumer has failed.  (any consumed offset has returned false or thrown an exception)
        /// </summary>
        /// <returns>bool</returns>
        bool HasConsumerFailed();

        /// <summary>
        /// Returns list of ConsumerAccessorModels with properties of each defined consumer.
        /// </summary>
        /// <returns>List<ConsumerAccessorModel></returns>
        List<ConsumerAccessorModel> GetConsumerListJson();
    }

    public sealed class ConsumerAccessor : IConsumerAccessor
    {
        private readonly ILogger<ConsumerAccessor> _logger;
        private readonly IServiceProvider _serviceProvider;
        public ConsumerAccessor(ILogger<ConsumerAccessor> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
        }

        public bool HasConsumerFailed()
        {

            var consumerMembers = _serviceProvider.GetServices<ConsumerMember>().Where(x => x.ConsumerModel.Active);

            if (consumerMembers != null && consumerMembers.Any(x => !x.ConsumerIsActive))
            {
                _logger.LogTrace("KafkaHelper | ConsumerAccessor | HasConsumerFailed {true}", true);
                return true;
            }
            return false;
        }

        public List<ConsumerAccessorModel> GetConsumerListJson()
        {
            var consumerMembers = _serviceProvider.GetServices<ConsumerMember>();
            var consumerAccessors = new List<ConsumerAccessorModel>();

            if (consumerMembers != null && consumerMembers.Any())
            {
                foreach (var consumerMember in consumerMembers)
                {
                    var obj = new ConsumerAccessorModel
                    {
                        IsActive = consumerMember.ConsumerIsActive,
                        DateTimeLastCommit = consumerMember.DateTimeLastCommit,
                        ConsumerModel = consumerMember.ConsumerModel
                    };

                    consumerAccessors.Add(obj);
                }
            }

            return consumerAccessors;
        }
    }
}
