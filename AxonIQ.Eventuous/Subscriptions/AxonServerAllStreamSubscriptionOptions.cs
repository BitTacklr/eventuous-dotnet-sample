using AxonIQ.AxonServer.Connector;
using Eventuous.Subscriptions;

namespace AxonIQ.Eventuous.Subscriptions;

public record AxonServerAllStreamSubscriptionOptions : SubscriptionOptions
{
    public PermitCount BufferSize { get; set; }
    public PermitCount RefillBatch { get; set; }
    public int ConcurrencyLimit { get; set; } = 1;
}