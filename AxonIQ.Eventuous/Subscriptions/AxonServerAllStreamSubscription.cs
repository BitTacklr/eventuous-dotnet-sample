using AxonIQ.AxonServer.Connector;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Subscriptions.Context;
using Eventuous.Subscriptions.Filters;
using Io.Axoniq.Axonserver.Grpc.Event;
using Microsoft.Extensions.Logging;

namespace AxonIQ.Eventuous.Subscriptions;

public class AxonServerAllStreamSubscription : EventSubscriptionWithCheckpoint<AxonServerAllStreamSubscriptionOptions>
{
    private readonly AxonServerConnectionFactory _factory;

    public AxonServerAllStreamSubscription(AxonServerConnectionFactory factory, AxonServerAllStreamSubscriptionOptions options, ICheckpointStore checkpointStore, ConsumePipe consumePipe, ILoggerFactory? loggerFactory) : base(options, checkpointStore, consumePipe, options.ConcurrencyLimit, loggerFactory)
    {
        _factory = factory ?? throw new ArgumentNullException(nameof(factory));
    }
    
    private async Task<IAxonServerConnection> GetConnection(CancellationToken ct)
    {
        var connection = await
            _factory
                .ConnectAsync(Options.Context, ct);
        await connection.WaitUntilReadyAsync();
        return connection;
    }

    protected override async ValueTask Subscribe(CancellationToken cancellationToken)
    {
        var checkpoint = await GetCheckpoint(cancellationToken).ConfigureAwait(false);
        var token = checkpoint.Position.HasValue ? new EventStreamToken(Convert.ToInt64(checkpoint.Position.Value)) : EventStreamToken.None;
        EventStream = (await GetConnection(cancellationToken)).EventChannel.OpenStream(token, Options.BufferSize, Options.RefillBatch);
        MessagePump = PumpEvents(EventStream, cancellationToken);

        async Task PumpEvents(IEventStream stream, CancellationToken ct)
        {
            try
            {
                await foreach (var @event in stream)
                {
                    await HandleInternal(CreateContext(@event, ct));
                }
            }
            catch (OperationCanceledException exception) when (exception.CancellationToken == ct)
            {
            }
        }
    }

    private IMessageConsumeContext CreateContext(
        EventWithToken @event,
        CancellationToken cancellationToken)
    {
        var message = DeserializeData(EventContentType.Json, @event.Event.Payload.Type, @event.Event.Payload.Data.Memory, @event.Event.AggregateIdentifier, (ulong) @event.Token);
        return new MessageConsumeContext(
            @event.Event.MessageIdentifier,
            @event.Event.Payload.Type,
            EventContentType.Json,
            @event.Event.AggregateIdentifier,
            (ulong)@event.Event.AggregateSequenceNumber,
            (ulong)@event.Token,
            (ulong)@event.Token,
            DateTimeOffset.FromUnixTimeMilliseconds(@event.Event.Timestamp).Date,
            message,
            @event.Event.ReadAsEventuousMetadata(),
            SubscriptionId, cancellationToken);
    }

    protected override async ValueTask Unsubscribe(CancellationToken cancellationToken)
    {
        try
        {
            Stopping.Cancel(false);
            await Task.Delay(100, cancellationToken);
            EventStream?.Dispose();
            if (MessagePump != null)
            {
                await MessagePump;    
            }
        }
        catch
        {
            // ignored
        }
    }

    protected override EventPosition GetPositionFromContext(IMessageConsumeContext context) => EventPosition.FromAllContext(context);

    private IEventStream? EventStream { get; set; }
    private Task? MessagePump { get; set; }
}