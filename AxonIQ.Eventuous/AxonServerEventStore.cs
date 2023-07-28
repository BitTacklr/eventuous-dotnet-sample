using System.Net.Mime;
using System.Runtime.Serialization;
using AxonIQ.AxonServer.Connector;
using Eventuous;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Io.Axoniq.Axonserver.Grpc;
using Io.Axoniq.Axonserver.Grpc.Event;
using Microsoft.Extensions.Logging;

namespace AxonIQ.Eventuous;

public class AxonServerEventStore : IEventStore
{
    private readonly AxonServerConnection _connection;
    private readonly IEventSerializer _serializer;
    private readonly ILogger<AxonServerEventStore>? _logger;
    private readonly Func<DateTimeOffset> _clock;

    public AxonServerEventStore(
        AxonServerConnection           connection,
        IEventSerializer?              serializer     = null,
        Func<DateTimeOffset>?          clock          = null,
        ILogger<AxonServerEventStore>? logger         = null)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _serializer = serializer ?? DefaultEventSerializer.Instance;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
        _logger = logger;
    }
    
    public async Task<bool> StreamExists(StreamName stream, CancellationToken cancellationToken) =>
        await 
            _connection
                .EventChannel
                .OpenStream(
                    new AxonServer.Connector.AggregateId(stream.GetId()),
                    new EventSequenceNumber(0))
                .AnyAsync(cancellationToken)
                .ConfigureAwait(false);

    public async Task<StreamEvent[]> ReadEvents(StreamName stream, StreamReadPosition start, int count, CancellationToken cancellationToken) =>
        await
            _connection
                .EventChannel
                .OpenStream(
                    new AxonServer.Connector.AggregateId(stream.GetId()),
                    new EventSequenceNumber(start.Value),
                    new EventSequenceNumber(start.Value + count))
                .Select(ToStreamEvent)
                .ToArrayAsync(cancellationToken)
                .ConfigureAwait(false);

    public Task<StreamEvent[]> ReadEventsBackwards(StreamName stream, int count, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("AxonServer does not support reading streams backwards");
    }

    private StreamEvent ToStreamEvent(Event @event) =>
        new(
            Guid.Parse(@event.MessageIdentifier),
            _serializer.DeserializeEvent(@event.Payload.Data.Span, @event.Payload.Type, EventContentType.Json) switch
            {
                SuccessfullyDeserialized success => success.Payload,
                FailedToDeserialize failed => throw new SerializationException(
                    $"Can't deserialize {@event.Payload.Type}: {failed.Error}"
                ),
                _ => throw new Exception("Unknown deserialization result"),
            },
            @event.ReadAsEventuousMetadata(),
            EventContentType.Json,
            @event.AggregateSequenceNumber
        );

    public async Task<AppendEventsResult> AppendEvents(
        StreamName stream, 
        ExpectedStreamVersion expectedVersion, IReadOnlyCollection<StreamEvent> events,
        CancellationToken cancellationToken)
    {
        var appendable = events.Select((@event, index) => ToEvent(stream, expectedVersion.Value + index + 1, @event)).ToArray();
        
        using var transaction = _connection.EventChannel.StartAppendEventsTransaction();
        foreach (var @event in appendable)
        {
            await transaction.AppendEventAsync(@event).ConfigureAwait(false);
        }
        var committed = await transaction.CommitAsync().ConfigureAwait(false);
        if (!committed.Success)
        {
            throw new AppendToStreamException(stream, new Exception("The append operation failed to commit"));
        }

        return new AppendEventsResult(
            0UL,
            appendable.Length > 0 ? appendable[^1].AggregateSequenceNumber : -1L);
    }

    private Event ToEvent(StreamName stream, long sequence, StreamEvent @event)
    {
        var output = new Event
        {
            MessageIdentifier = @event.Id.ToString("D"),
            AggregateIdentifier = stream.GetId(),
            AggregateSequenceNumber = sequence,
            Snapshot = false,
            Timestamp = _clock().ToUnixTimeMilliseconds()
        };
        if (@event.Payload != null)
        {
            var serialized = _serializer.SerializeEvent(@event.Payload);
            output.Payload = new SerializedObject
            {
                Data = ByteString.CopyFrom(serialized.Payload),
                Type = serialized.EventType
            };
        }
        
        if (TryReadAggregateType(stream, out var type))
        {
            output.AggregateType = type;
        }
        WriteMetadata(output.MetaData, @event.Metadata);
        return output;
    }
    
    private static bool TryReadAggregateType(StreamName name, out string? aggregateType)
    {
        var raw = name.ToString();
        var indexOfDash = raw.IndexOf('-');
        if (indexOfDash != -1)
        {
            aggregateType = raw[..(indexOfDash - 1)];
            return true;
        }

        aggregateType = null;
        return false;
    }
    
    private static void WriteMetadata(MapField<string, MetaDataValue> field, Metadata metadata)
    {
        foreach (var datum in metadata)
        {
            var metaDataValue = new MetaDataValue();
            switch (datum.Value)
            {
                case string value:
                    metaDataValue.TextValue = value;
                    break;
                case int value:
                    metaDataValue.NumberValue = value;
                    break;
                case long value:
                    metaDataValue.NumberValue = value;
                    break;
                case double value:
                    metaDataValue.DoubleValue = value;
                    break;
                case float value:
                    metaDataValue.DoubleValue = value;
                    break;
                case bool value:
                    metaDataValue.BooleanValue = value;
                    break;
                case byte[] value:
                    metaDataValue.BytesValue = new SerializedObject
                    {
                        Data = ByteString.CopyFrom(value),
                        Type = MediaTypeNames.Application.Octet
                    };
                    break;
                case null:
                    metaDataValue.ClearData();
                    break;
                default:
                    throw new SerializationException(
                $"Can't serialize metadata key {datum.Key} because its value type {datum.Value?.GetType().Name ?? ""} is not supported");
            }
            field.Add(datum.Key, metaDataValue);
        }
    }

    public Task TruncateStream(StreamName stream, StreamTruncatePosition truncatePosition, ExpectedStreamVersion expectedVersion,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException("AxonServer does not support the truncation of an event stream.");
    }

    public Task DeleteStream(StreamName stream, ExpectedStreamVersion expectedVersion, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("AxonServer does not support the deletion of an event stream.");
    }
}