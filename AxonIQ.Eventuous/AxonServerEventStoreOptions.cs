using AxonIQ.AxonServer.Connector;

namespace AxonIQ.Eventuous;

public record AxonServerEventStoreOptions
{
    public Context Context { get; set; } = Context.Default;
}