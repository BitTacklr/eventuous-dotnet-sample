using AxonIQ.AxonServer.Connector;

namespace AxonIQ.Eventuous.Producers;

public record AxonServerProducerOptions
{
    public Context Context { get; set; } = Context.Default;
    public int MaxAppendsEventsCount { get; set; } 
}