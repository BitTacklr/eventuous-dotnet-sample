using System.Text.Json;
using AxonIQ.AxonServer.Connector;
using AxonIQ.Eventuous;
using AxonIQ.Eventuous.Subscriptions;
using Bookings.Application;
using Bookings.Application.Queries;
using Bookings.Domain;
using Bookings.Domain.Bookings;
using Bookings.Infrastructure;
using Bookings.Integration;
using Eventuous;
using Eventuous.Diagnostics.OpenTelemetry;
using Eventuous.Projections.MongoDB;
using Eventuous.Subscriptions.Registrations;
using NodaTime;
using NodaTime.Serialization.SystemTextJson;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Bookings;

public static class Registrations {
    public static void AddEventuous(this IServiceCollection services, IConfiguration configuration) {
        DefaultEventSerializer.SetDefaultSerializer(
            new DefaultEventSerializer(
                new JsonSerializerOptions(JsonSerializerDefaults.Web).ConfigureForNodaTime(
                    DateTimeZoneProviders.Tzdb
                )
            )
        );

        services.AddAxonServerConnectionFactory();
        services.AddSingleton<AxonServerEventStoreOptions>();
        services.AddAggregateStore<AxonServerEventStore>();
        services.AddCommandService<BookingsCommandService, Booking>();

        services.AddSingleton<Services.IsRoomAvailable>((id, period) => new ValueTask<bool>(true));

        services.AddSingleton<Services.ConvertCurrency>((from, currency)
            => new Money(from.Amount * 2, currency)
        );

        services.AddSingleton(Mongo.ConfigureMongo(configuration));
        services.AddCheckpointStore<MongoCheckpointStore>();
        
        services.AddSubscription<AxonServerAllStreamSubscription, AxonServerAllStreamSubscriptionOptions>(
            "BookingsProjections",
            builder => builder
                .Configure(options =>
                {
                    options.BufferSize = new PermitCount(1024);
                    options.RefillBatch = new PermitCount(1024);
                })
                .UseCheckpointStore<MongoCheckpointStore>()
                .AddEventHandler<BookingStateProjection>()
                .AddEventHandler<MyBookingsProjection>()
                .WithPartitioningByStream(2)
        );

        services.AddSubscription<AxonServerStreamSubscription, AxonServerStreamSubscriptionOptions>(
            "PaymentIntegration",
            builder => builder
                .Configure(x => x.StreamName = PaymentsIntegrationHandler.Stream)
                .AddEventHandler<PaymentsIntegrationHandler>()
        );
    }

    public static void AddTelemetry(this IServiceCollection services) {
        var otelEnabled = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT") != null;

        services.AddOpenTelemetry()
            .WithMetrics(
                builder => {
                    builder
                        .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("bookings"))
                        .AddAspNetCoreInstrumentation()
                        .AddEventuous()
                        .AddEventuousSubscriptions()
                        .AddPrometheusExporter();
                    if (otelEnabled) builder.AddOtlpExporter();
                }
            );

        services.AddOpenTelemetry()
            .WithTracing(
                builder => {
                    builder
                        .AddAspNetCoreInstrumentation()
                        .AddGrpcClientInstrumentation()
                        .AddEventuousTracing()
                        .AddMongoDBInstrumentation()
                        .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("bookings"))
                        .SetSampler(new AlwaysOnSampler());

                    if (otelEnabled)
                        builder.AddOtlpExporter();
                    else
                        builder.AddZipkinExporter();
                }
            );
    }
}
