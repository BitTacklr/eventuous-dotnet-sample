using AxonIQ.AxonServer.Connector;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Subscriptions.Context;
using Eventuous.Subscriptions.Filters;
using Io.Axoniq.Axonserver.Grpc.Event;
using Microsoft.Extensions.Logging;

namespace AxonIQ.Eventuous.Subscriptions;

public class AxonServerStreamSubscription : EventSubscriptionWithCheckpoint<AxonServerStreamSubscriptionOptions>
{
    private readonly AxonServerConnection _connection;

    public AxonServerStreamSubscription(AxonServerConnection connection, AxonServerStreamSubscriptionOptions options, ICheckpointStore checkpointStore, ConsumePipe consumePipe, ILoggerFactory? loggerFactory) 
        : base(options, checkpointStore, consumePipe, options.ConcurrencyLimit, loggerFactory)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    protected override async ValueTask Subscribe(CancellationToken cancellationToken)
    {
        var checkpoint = await GetCheckpoint(cancellationToken).ConfigureAwait(false);
        var token = checkpoint.Position.HasValue ? new EventSequenceNumber(Convert.ToInt64(checkpoint.Position.Value)) : new EventSequenceNumber(0);
        EventStream = _connection.EventChannel.OpenStream(new AggregateId(Options.StreamName.GetId()), token);
        MessagePump = PumpEvents(EventStream, cancellationToken);

        async Task PumpEvents(IAggregateEventStream stream, CancellationToken ct)
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
        Event @event,
        CancellationToken cancellationToken)
    {
        var message = DeserializeData(EventContentType.Json, @event.Payload.Type, @event.Payload.Data.Memory, @event.AggregateIdentifier, (ulong) @event.AggregateSequenceNumber);
        return new MessageConsumeContext(
            @event.MessageIdentifier,
            @event.Payload.Type,
            EventContentType.Json,
            @event.AggregateIdentifier,
            (ulong)@event.AggregateSequenceNumber,
            (ulong)@event.AggregateSequenceNumber,
            (ulong)@event.AggregateSequenceNumber,
            DateTimeOffset.FromUnixTimeMilliseconds(@event.Timestamp).Date,
            message,
            @event.ReadAsEventuousMetadata(),
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

    private IAggregateEventStream? EventStream { get; set; }
    private Task? MessagePump { get; set; }
}