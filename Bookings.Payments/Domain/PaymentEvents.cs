using Eventuous;

namespace Bookings.Payments.Domain;

public static class PaymentEvents {
    [EventType("V1.PaymentRecorded")]
    public record PaymentRecorded(
        string PaymentId, string BookingId, float Amount, string Currency, string Method, string Provider
    );
}