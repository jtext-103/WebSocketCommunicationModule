namespace Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics
{
    public sealed class WebSocketServerSessionStatus
    {
        public Guid SessionId { get; init; }

        public string State { get; init; } = string.Empty;

        public string? RemoteAddress { get; init; }

        public DateTime CreatedAtUtc { get; init; }

        public DateTime? HandshakeCompletedAtUtc { get; init; }

        public DateTime LastReceivedAtUtc { get; init; }

        public int RemoteSubscriptionCount { get; init; }
    }
}
