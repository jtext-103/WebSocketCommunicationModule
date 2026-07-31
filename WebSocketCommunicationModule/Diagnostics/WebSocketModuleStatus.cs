namespace Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics
{
    public sealed class WebSocketModuleStatus
    {
        public DateTime SnapshotAtUtc { get; init; }

        public string State { get; init; } = string.Empty;

        public bool Started { get; init; }

        public bool Disposed { get; init; }

        public bool ServerEnabled { get; init; }

        public bool ServerStarted { get; init; }

        public string? ListenUri { get; init; }

        public int ServerActiveSessionCount { get; init; }

        public int ServerRemoteSubscriptionCount { get; init; }

        public int ServerAggregatedSubscriptionCount { get; init; }

        public int ActiveFanOutEventCount { get; init; }

        public int RemoteClientCount { get; init; }

        public int RemoteConnectedClientCount { get; init; }

        public int RemoteFailedClientCount { get; init; }

        public int RemoteSubscriptionCount { get; init; }

        public int RemoteActiveSubscriptionCount { get; init; }

        public int RemoteFailedSubscriptionCount { get; init; }

        public int ClientAggregationGroupCount { get; init; }

        public int LocalSubscriptionCount { get; init; }
    }
}
