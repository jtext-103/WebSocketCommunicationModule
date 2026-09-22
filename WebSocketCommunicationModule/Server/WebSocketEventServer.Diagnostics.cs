using Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server
{
    public sealed partial class WebSocketEventServer
    {
        internal int ActiveSessionCount => sessions.Count;

        internal int RemoteSubscriptionCount => subscriptionManager.RemoteSubscriptionCount;

        internal int AggregatedSubscriptionCount => subscriptionManager.AggregatedSubscriptionCount;

        internal int ActiveFanOutEventCount => subscriptionManager.ActiveFanOutEventCount;

        internal IReadOnlyList<WebSocketServerSessionStatus> GetSessionStatuses()
        {
            return sessions.Values
                .Select(session => session.GetDiagnosticStatus())
                .OrderBy(status => status.CreatedAtUtc)
                .ToArray();
        }
    }
}
