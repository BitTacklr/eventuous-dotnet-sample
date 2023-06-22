using System.Runtime.Serialization;
using Eventuous;
using Google.Protobuf;
using Io.Axoniq.Axonserver.Grpc;
using Io.Axoniq.Axonserver.Grpc.Event;

namespace AxonIQ.Eventuous;

internal static class MetadataExtensions
{
    public static Metadata ReadAsEventuousMetadata(this Event @event)
    {
        if (@event.MetaData == null || @event.MetaData.Count == 0)
        {
            return Metadata.FromHeaders(null);
        }
        return Metadata.FromHeaders(new Dictionary<string, string?>(@event
            .MetaData
            .Select(
                datum => new KeyValuePair<string, string?>(datum.Key, datum.Value.DataCase switch
                {
                    MetaDataValue.DataOneofCase.None => null,
                    MetaDataValue.DataOneofCase.TextValue => datum.Value.TextValue,
                    MetaDataValue.DataOneofCase.NumberValue => datum.Value.NumberValue.ToString(),
                    MetaDataValue.DataOneofCase.BooleanValue => datum.Value.BooleanValue.ToString(),
                    MetaDataValue.DataOneofCase.DoubleValue => datum.Value.DoubleValue.ToString("R"),
                    MetaDataValue.DataOneofCase.BytesValue => datum.Value.BytesValue.ToByteString().ToBase64(),
                    _ => throw new SerializationException(
                        $"Can't deserialize metadata of event {@event.Payload.Type}: {datum.Value.DataCase.ToString()} not supported")
                })
            )
        ));
    }
}