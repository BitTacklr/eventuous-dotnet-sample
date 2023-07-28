using System.Text.Json;
using AxonIQ.AxonServer.Connector;
using AxonIQ.Eventuous;
using AxonIQ.Eventuous.Producers;
using AxonIQ.Eventuous.Subscriptions;
using Bookings.Payments.Application;
using Bookings.Payments.Domain;
using Bookings.Payments.Infrastructure;
using Bookings.Payments.Integration;
using Eventuous;
using Eventuous.Diagnostics.OpenTelemetry;
using Eventuous.Projections.MongoDB;
using NodaTime;
using NodaTime.Serialization.SystemTextJson;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Bookings.Payments;

public static class Registrations {
    public static void AddServices(this IServiceCollection services, IConfiguration configuration) {
        DefaultEventSerializer.SetDefaultSerializer(
            new DefaultEventSerializer(
                new JsonSerializerOptions(JsonSerializerDefaults.Web).ConfigureForNodaTime(
                    DateTimeZoneProviders.Tzdb
                )
            )
        );
        
        services.AddAxonServerConnection(Context.Default, 
            options => options.AsComponentName(new ComponentName("Payments")));
        services.AddAggregateStore<AxonServerEventStore>();
        services.AddCommandService<CommandService, Payment>();
        services.AddSingleton(Mongo.ConfigureMongo(configuration));
        services.AddCheckpointStore<MongoCheckpointStore>();
        services.AddEventProducer<AxonServerProducer>();

        services
            .AddGateway<AxonServerAllStreamSubscription, AxonServerAllStreamSubscriptionOptions, AxonServerProducer>(
                "IntegrationSubscription",
                PaymentsGateway.Transform,
                options =>
                {
                    options.BufferSize = new PermitCount(1024);
                    options.RefillBatch = new PermitCount(1024);
                }
            );
    }

    public static void AddTelemetry(this IServiceCollection services) {
        services.AddOpenTelemetry()
            .WithMetrics(
                builder => builder
                    .AddAspNetCoreInstrumentation()
                    .AddEventuous()
                    .AddEventuousSubscriptions()
                    .AddPrometheusExporter()
            );

        services.AddOpenTelemetry()
            .WithTracing(
                builder => builder
                    .AddAspNetCoreInstrumentation()
                    .AddGrpcClientInstrumentation()
                    .AddEventuousTracing()
                    .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("payments"))
                    .SetSampler(new AlwaysOnSampler())
                    .AddZipkinExporter()
            );
    }
}
