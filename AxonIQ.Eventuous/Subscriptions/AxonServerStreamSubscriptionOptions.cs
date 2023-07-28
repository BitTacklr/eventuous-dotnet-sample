using AxonIQ.AxonServer.Connector;
using Eventuous;
using Eventuous.Subscriptions;

namespace AxonIQ.Eventuous.Subscriptions;

public record AxonServerStreamSubscriptionOptions : SubscriptionOptions
{
    public StreamName StreamName { get; set; }
    public int ConcurrencyLimit { get; set; } = 1;
}