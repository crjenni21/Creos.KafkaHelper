using Serilog;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Builder;
using Creos.KafkaHelper.TestApp.HostedServices;
using Creos.KafkaHelper.Extentions;
using Microsoft.Extensions.DependencyInjection;

namespace Creos.KafkaHelper.TestApp
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

            var builder = WebApplication.CreateBuilder(args);
            builder.Host.UseSerilog();

            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(builder.Configuration)
                .CreateLogger();

            builder.Services.AddKafkaHelper(builder.Configuration);
            builder.Services.AddHostedService<ConsumerHostedService>();
            builder.Services.AddHostedService<ConsumerHostedService2>();
            builder.Services.AddHostedService<ProducerExampleService>();
            builder.Services.AddControllers();


            var app = builder.Build();
            app.UseRouting();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
            app.UseSerilogRequestLogging();
            app.Run();

        }

    }
}
