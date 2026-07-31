namespace Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics
{
    public sealed class WebSocketRemoteClientStatus
    {
        public string Endpoint { get; init; } = string.Empty;

        public string State { get; init; } = string.Empty;

        public DateTime? ConnectedAtUtc { get; init; }

        public DateTime? LastReceivedAtUtc { get; init; }

        public int RetryCount { get; init; }

        public string? LastError { get; init; }

        public int ActionQueueLength { get; init; }

        public int SubscriptionCount { get; init; }

        public int PendingSubscriptionCount { get; init; }

        public int ActiveSubscriptionCount { get; init; }

        public int FailedSubscriptionCount { get; init; }

        public IReadOnlyList<WebSocketRemoteSubscriptionStatus> Subscriptions { get; init; }
            = Array.Empty<WebSocketRemoteSubscriptionStatus>();
    }
}
