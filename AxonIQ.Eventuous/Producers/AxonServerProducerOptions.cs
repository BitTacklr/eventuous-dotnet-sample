namespace AxonIQ.Eventuous.Producers;

public record AxonServerProducerOptions
{
    public int MaxAppendsEventsCount { get; set; } 
}