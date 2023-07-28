using System.Net.Mime;
using System.Runtime.Serialization;
using AxonIQ.AxonServer.Connector;
using Eventuous;
using Eventuous.Producers;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Io.Axoniq.Axonserver.Grpc;
using Io.Axoniq.Axonserver.Grpc.Event;
using Microsoft.Extensions.Logging;

namespace AxonIQ.Eventuous.Producers;

public class AxonServerProducer : BaseProducer<AxonServerProducerOptions>
{
    private readonly AxonServerConnection _connection;
    private readonly IEventSerializer _serializer;
    private readonly Func<DateTimeOffset> _clock;
    private readonly ILogger<AxonServerProducer>? _logger;

    public AxonServerProducer(
        AxonServerConnection         connection,
        IEventSerializer?            serializer = null,
        Func<DateTimeOffset>?        clock = null,
        ILogger<AxonServerProducer>? logger = null)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _serializer = serializer ?? DefaultEventSerializer.Instance;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
        _logger = logger;
    }
    
    protected override async Task ProduceMessages(
        StreamName stream, 
        IEnumerable<ProducedMessage> messages, 
        AxonServerProducerOptions? options,
        CancellationToken cancellationToken = default)
    {
        foreach (var chunk in messages.Chunk(options?.MaxAppendsEventsCount ?? 1024))
        {
            try
            {
                using var transaction =
                    _connection
                    .EventChannel
                    .StartAppendEventsTransaction();
                foreach (var message in chunk)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await transaction.AppendEventAsync(ToEvent(stream, message));
                }

                (await transaction.CommitAsync()).EnsureSuccess();
                
                foreach (var message in chunk)
                {
                    await message.Ack<AxonServerProducer>();
                }
            }
            catch (Exception exception)
            {
                foreach (var message in chunk)
                {
                    await message.Nack<AxonServerProducer>("Unable to produce to AxonServer", exception);
                }
            }
        }
    }

    private Event ToEvent(StreamName stream, ProducedMessage message)
    {
        var output = new Event
        {
            MessageIdentifier = message.MessageId.ToString("D"),
            AggregateIdentifier = stream.GetId(),
            Snapshot = false,
            Timestamp = _clock().ToUnixTimeMilliseconds()
        };
        if (TryReadAggregateType(stream, out var type))
        {
            output.AggregateType = type;
        }

        var serialized = _serializer.SerializeEvent(message.Message);
        output.Payload = new SerializedObject
        {
            Data = ByteString.CopyFrom(serialized.Payload),
            Type = serialized.EventType
        };

        if (message.Metadata != null)
        {
            WriteMetadata(output.MetaData, message.Metadata);
        }

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
}