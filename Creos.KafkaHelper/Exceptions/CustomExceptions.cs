
using System.Configuration;

namespace Creos.KafkaHelper.Exceptions
{
    [Serializable]
    internal class ConfigurationMissingException : ConfigurationErrorsException
    {
        public ConfigurationMissingException() { }
        public ConfigurationMissingException(string s) : base(s) { }
    }

    [Serializable]
    internal class KafkaBrokerMissingException : ConfigurationErrorsException
    {
        public KafkaBrokerMissingException() { }
        public KafkaBrokerMissingException(string s) : base(s) { }
    }

    [Serializable]
    internal class KafkaBrokerInvalidException : ConfigurationErrorsException
    {
        public KafkaBrokerInvalidException() { }
        public KafkaBrokerInvalidException(string s) : base(s) { }
    }

    [Serializable]
    internal class KafkaProducerNameNotFoundException : ConfigurationErrorsException
    {
        public KafkaProducerNameNotFoundException() { }
        public KafkaProducerNameNotFoundException(string s) : base(s) { }
    }

    [Serializable]
    internal class KafkaTopicNameNotAllowedException : ConfigurationErrorsException
    {
        public KafkaTopicNameNotAllowedException() { }
        public KafkaTopicNameNotAllowedException(string s) : base(s) { }
    }

    [Serializable]
    internal class KafkaConsumerInvalidConfiguration : ConfigurationErrorsException
    {
        public KafkaConsumerInvalidConfiguration() { }
        public KafkaConsumerInvalidConfiguration(string s) : base(s) { }
    }

}
