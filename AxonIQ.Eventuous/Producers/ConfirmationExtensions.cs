using Io.Axoniq.Axonserver.Grpc.Event;

namespace AxonIQ.Eventuous.Producers;

internal static class ConfirmationExtensions
{
    public static void EnsureSuccess(this Confirmation confirmation)
    {
        if (!confirmation.Success)
        {
            throw new Exception("The transaction failed to commit");
        }
    }
}