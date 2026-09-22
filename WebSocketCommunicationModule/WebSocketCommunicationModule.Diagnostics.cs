using Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;

namespace Jtext103.CFET2.WebSocketCommunicationModule
{
    public partial class WebSocketCommunicationModule
    {
        public WebSocketModuleStatus GetStatus()
        {
            var remoteClients = GetRemoteClients();
            var currentServer = server;
            return new WebSocketModuleStatus
            {
                SnapshotAtUtc = DateTime.UtcNow,
                State = disposed ? "Disposed" : started ? "Started" : "Created",
                Started = started,
                Disposed = disposed,
                ServerEnabled = options.EnableServer,
                ServerStarted = currentServer?.IsStarted == true,
                ListenUri = options.ListenUri?.ToString(),
                ServerActiveSessionCount = currentServer?.ActiveSessionCount ?? 0,
                ServerRemoteSubscriptionCount = currentServer?.RemoteSubscriptionCount ?? 0,
                ServerAggregatedSubscriptionCount = currentServer?.AggregatedSubscriptionCount ?? 0,
                ActiveFanOutEventCount = currentServer?.ActiveFanOutEventCount ?? 0,
                RemoteClientCount = remoteClients.Count,
                RemoteConnectedClientCount = remoteClients.Count(client => client.State == RemoteConnectionState.Connected.ToString()),
                RemoteFailedClientCount = remoteClients.Count(client => client.State == RemoteConnectionState.Failed.ToString()),
                RemoteSubscriptionCount = remoteClients.Sum(client => client.SubscriptionCount),
                RemotePendingSubscriptionCount = remoteClients.Sum(client => client.PendingSubscriptionCount),
                RemoteActiveSubscriptionCount = remoteClients.Sum(client => client.ActiveSubscriptionCount),
                RemoteFailedSubscriptionCount = remoteClients.Sum(client => client.FailedSubscriptionCount),
                ClientAggregationGroupCount = subscriptionAggregator.GroupCount,
                LocalSubscriptionCount = subscriptionAggregator.LocalSubscriptionCount,
            };
        }

        public string PrintStatus()
        {
            var status = GetStatus();
            return $"state={status.State} started={status.Started} disposed={status.Disposed} " +
                   $"serverSessions={status.ServerActiveSessionCount} " +
                   $"serverRemoteSubscriptions={status.ServerRemoteSubscriptionCount} " +
                   $"serverAggregatedSubscriptions={status.ServerAggregatedSubscriptionCount} " +
                   $"remoteClients={status.RemoteClientCount} remoteSubscriptions={status.RemoteSubscriptionCount} " +
                   $"remotePending={status.RemotePendingSubscriptionCount} " +
                   $"remoteActive={status.RemoteActiveSubscriptionCount} " +
                   $"remoteFailed={status.RemoteFailedSubscriptionCount}";
        }

        public IReadOnlyList<WebSocketServerSessionStatus> GetServerSessions()
        {
            return server?.GetSessionStatuses() ?? Array.Empty<WebSocketServerSessionStatus>();
        }

        public string PrintServerSessions()
        {
            return string.Join(
                Environment.NewLine,
                GetServerSessions().Select(session =>
                    $"session={session.SessionId} state={session.State} remote={session.RemoteAddress ?? "unknown"} " +
                    $"subscriptions={session.RemoteSubscriptionCount} handshakeAt={session.HandshakeCompletedAtUtc:O} lastReceivedAt={session.LastReceivedAtUtc:O}"));
        }

        public IReadOnlyList<WebSocketRemoteClientStatus> GetRemoteClients()
        {
            return endpointClients.Values
                .Select(client => client.GetDiagnosticStatus())
                .OrderBy(client => client.Endpoint, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        public string PrintRemoteClients()
        {
            return string.Join(Environment.NewLine, GetRemoteClients().Select(FormatRemoteClient));
        }

        public WebSocketRemoteClientStatus? GetRemoteClient(string hostname)
        {
            if (string.IsNullOrWhiteSpace(hostname))
            {
                return null;
            }

            string endpoint;
            try
            {
                endpoint = RemoteEndpointKey.Parse(hostname).NormalizedHost;
            }
            catch (FormatException)
            {
                return null;
            }

            return GetRemoteClients().FirstOrDefault(client =>
                string.Equals(client.Endpoint, endpoint, StringComparison.OrdinalIgnoreCase));
        }

        public string PrintRemoteClient(string hostname)
        {
            var client = GetRemoteClient(hostname);
            return client == null ? string.Empty : FormatRemoteClient(client);
        }

        private static string FormatRemoteClient(WebSocketRemoteClientStatus client)
        {
            return $"endpoint={client.Endpoint} state={client.State} queue={client.ActionQueueLength} " +
                   $"subscriptions={client.SubscriptionCount} pending={client.PendingSubscriptionCount} " +
                   $"active={client.ActiveSubscriptionCount} failed={client.FailedSubscriptionCount} " +
                   $"retries={client.RetryCount} lastError={client.LastError ?? string.Empty}";
        }
    }
}
