namespace Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics
{
    public sealed class WebSocketRemoteSubscriptionStatus
    {
        public Guid TokenId { get; init; }

        public string Source { get; init; } = string.Empty;

        public string EventType { get; init; } = string.Empty;

        public int PerformanceLevel { get; init; }

        public int Revision { get; init; }

        public int? LastAcceptedRevision { get; init; }

        public string State { get; init; } = string.Empty;

        public DateTime CreatedAtUtc { get; init; }

        public DateTime? ActivatedAtUtc { get; init; }

        public DateTime? SubscribeSentAtUtc { get; init; }

        public int SubscribeRetryCount { get; init; }

        public string? LastError { get; init; }
    }
}
