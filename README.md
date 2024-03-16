# Creos.KafkaHelper

## Supported Frameworks:
 - .NET 6
 - .NET 8

## Overall 

This is a simple library that eases the producing and consuming from Kafka.  
It is designed to be configurable via Configuration and allows consuming and producing to multiple topics.

There are three base properties that are configurable within the Configuration:

```
  "KafkaConfiguration": {
    "Brokers": [],
    "Producers": [],
    "Consumers": []
  }

```

### Broker List
Brokers accepts a list of 1 or more brokers, depending on how large your cluster is.  

### Producer List
The Producers list is an optional list of producers within your appliction.  

This is designed such that, you can define a single producer, if you wish, and share those producer settings amongst multiple topics. 
Or if you wish, you can define a unique producer for each topic.  

### Consumer List
The Consumers list is an optional list of consumers within your application.  

Each consumer can be configured to consume from 1 or more topics.  
A topic name can even be wildcarded with an astricks to support multiple topics matching a pattern.

Multiple consumers are processed concurrently.   




## Configurable Properties

### Producer Properties

 - **ProducerName**: (Required) This must be unique amongst all Producers defined within your Configuration.  This is an arbitrary string that you will reference within your application to determine which ProducerSettings you want to Produce to.   
 - Brokers: (Optional) If this is set, this will override the Broker List defined in the parent configuration -- "KafkaConfiguration:Brokers".
 - Topic:  (Optional) This is the default topic that a Kafka Message is produced to if not explicitly defined within your application.   (Not required)
 - Active:  (Optional) Boolean (true or false) option where this Producer could be not used.  Default:  true
 - Partitioner:  (Optional) Possible settings are defined within the Confluent.Kafka.Partitioner enum:  Default: ConsistentRandom
 - LingerMS:  (Optional) Producer Linger defined in milliseconds.   Default: 1000
 - BatchSizeBytes:  (Optional)  Producer BatchSize in bytes.  Default: 1000000

### Consumer Properties

 - **ConsumerName**: (Required) This must be unique amongst all Consumers defined within your Configuration.  This is an arbitrary string that you will reference within your application to determine which Consumer you want to consumer from. 
 - Brokers: (Optional) If this is set, this will override the Broker List defined in the parent configuration -- "KafkaConfiguration:Brokers".
 - EnableAutoCommit:  (Optional)  While Kafka guarantees at-least-once delivery.  Setting this to false also empowers the application to guarantee at-least-once successful delivery.    Default: true
 - BatchOffsetsToCommit:  (Optional)  Default: 500
 - FrequencyToCommitMs:  (Optional)  This setting is ignored if EnableAutoCommit is set to true.   Else, this defines the frequency at which the most recently successful consumed offset (per partition) is committed.  Default: 1000
 - **GroupID**:  (Required)  This setting defines the name of the ConsumerGroup.
 - **Topics**: (Required)  This setting defines the topic or topics assigned to this consumer.  This does support a wildcarded topic name.  
 - Active:  (Optional) Boolean (true or false) option where this Consumer could be not used.  Default:  true
 - ConsumeFromDate: (Optional)  Experimental option that allows for an offset to be reset to a particular date.  This setting could potentially be removed in a future update. 
 - StatisticsIntervalMs:  (Optional)  Overrides default if explicitly set. 
 - SessionTimeoutMs:  (Optional)  Overrides default if explicitly set. 
 - EnablePartitionEof:  (Optional)  Overrides default if explicitly set. 
 - ReconnectBackoffMs:  (Optional)  Overrides default if explicitly set. 
 - HeartbeatIntervalMs:  (Optional)  Overrides default if explicitly set. 
 - ReconnectBackoffMaxMs:  (Optional)  Overrides default if explicitly set. 
 - FetchMaxBytes:  (Optional)  Overrides default if explicitly set. 
 - AutoCommitIntervalMs:  (Optional)  Overrides default if explicitly set. 



## Implementation

### Consumers

Start a new class that inherits from ConsumerBackgroundService.  This is a traditional .NET BackgroundService
Within the ConsumerBackgroundService contructor:
1. Pass in a reference to IServiceProvider
2. Use the IServiceProvider implemenation to get your applicable consumer instance via the Consumer:Name property defined in your Configuration
3. Expose your instance of ConsumerMember for later use.

```
    public class ConsumerHostedService : ConsumerBackgroundService
    {
        private readonly ILogger<ConsumerHostedService> _logger;
        private readonly ConsumerMember _consumerMember;
        private CancellationToken _token;

        public ConsumerHostedService(ILogger<ConsumerHostedService> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _consumerMember = serviceProvider.GetServices<ConsumerMember>().Where(x => x.ConsumerModel.Active && x.ConsumerModel.Name == "TestConsumer").FirstOrDefault();
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _token = cancellationToken;
            if (_consumerMember != null)
            {
                await _consumerMember.RegisterConsumerMemberAsync(cancellationToken);
                _consumerMember.ConsumeEvent += ProcessConsumedMessageAsync;
            }
        }

        protected override async Task<bool> ProcessConsumedMessageAsync(ConsumeTriggerEventArgs consumeTriggerEvent)
        {
            var consumeResult = consumeTriggerEvent.ConsumeResult;
            _logger.LogDebug("Topic: {Topic}, offset: {Offset}, TopicPartitionOffset: {TopicPartitionOffset}", consumeResult.Topic, consumeResult.Offset, consumeResult.TopicPartitionOffset);
            // Your code here:
            //  return await ProcessMessage(consumeResult);
        }
    }

```

You can have multiple instances of a BackgroundService (ConsumerBackgroundService) to process multiple consumers concurrently. 

### Producers
This is much simpler to use within your application.  Inject an instance of IKafkaProducer into your class and call one of the ProduceMessageToKafka methods.  

```
        [HttpPost("ProduceMessage")]
        public async Task<IActionResult> ProduceMessage()
        {
            _logger.LogDebug("Entered ConsumerInfo Controller");
            await _producer.ProduceMessageToKafkaAsync("Testing", new Message<string, string>
            {
                Key = "Test_Message",
                Value = "Whatever Value Here"
            });
            return Ok(_consumerAccessor.GetConsumerListJson());
        }
```

## Consumer Accessor
This is a class that enables you to access existing configurations and state of your consumers.  
There are two exposed methods:

### HasConsumerFailed()
This is a simple method that will return a bool if any active consumer has failed.  

### GetConsumerListJson()
This method will return an object of the current state of all consumers defined in your configuration.
Creos.KafkaHelper.Consumer.ConsumerAccessor is an injected transient.
The model returned: 

```
    public sealed class ConsumerAccessorModel
    {
        public bool IsActive { get; internal set; }
        public DateTime DateTimeLastCommit { get; internal set; }
        public ConsumerModel ConsumerModel { get; internal set; }
    }
```

