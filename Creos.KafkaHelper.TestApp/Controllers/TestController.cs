
using Confluent.Kafka;
using Creos.KafkaHelper.Consumer;
using Creos.KafkaHelper.Producer;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Creos.KafkaHelper.TestApp.Controllers
{
    [ApiController]
    [Route("[Controller]")]
    public class TestController : ControllerBase
    {
        private readonly ILogger<TestController> _logger;
        private readonly IConsumerAccessor _consumerAccessor;
        private readonly IKafkaProducer _producer;

        public TestController(ILogger<TestController> logger, IConsumerAccessor consumerAccessor, IKafkaProducer kafkaProducer)
        {
            _logger = logger;
            _consumerAccessor = consumerAccessor;
            _producer = kafkaProducer;
        }

        [HttpGet("ConsumerInfo")]
        public IActionResult ConsumerInfo()
        {
            _logger.LogDebug("Entered TestController.ConsumerInfo");
            return Ok(_consumerAccessor.GetConsumerListJson());
        }

        [HttpPost("ProduceMessage")]
        public async Task<IActionResult> ProduceMessage()
        {
            _logger.LogDebug("Entered TestController.ProduceMessage");
            await _producer.ProduceMessageToKafkaAsync("Testing", new Message<string, string>
            {
                Key = "Test_Message",
                Value = "Whatever Value Here"
            });
            return Ok(_consumerAccessor.GetConsumerListJson());
        }

    }
}
