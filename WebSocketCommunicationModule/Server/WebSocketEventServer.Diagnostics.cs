using Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server
{
    public sealed partial class WebSocketEventServer
    {
        internal int ActiveSessionCount => sessions.Count;

        internal int RemoteSubscriptionCount => subscriptionManager.RemoteSubscriptionCount;

        internal int AggregatedSubscriptionCount => subscriptionManager.AggregatedSubscriptionCount;

        internal int ActiveFanOutEventCount => subscriptionManager.ActiveFanOutEventCount;

        internal int PendingFanOutEventCount => subscriptionManager.PendingFanOutEventCount;
        internal int PeakPendingFanOutEventCount => subscriptionManager.PeakPendingFanOutEventCount;
        internal long FanOutQueueFullDropCount => subscriptionManager.FanOutQueueFullDropCount;
        internal long FanOutShutdownDiscardCount => subscriptionManager.FanOutShutdownDiscardCount;
        internal int MaxPendingFanOutEvents => subscriptionManager.MaxPendingFanOutEvents;
        internal int MaxConcurrentFanOutEvents => subscriptionManager.MaxConcurrentFanOutEvents;

        internal IReadOnlyList<WebSocketServerSessionStatus> GetSessionStatuses()
        {
            return sessions.Values
                .Select(session => session.GetDiagnosticStatus())
                .OrderBy(status => status.CreatedAtUtc)
                .ToArray();
        }
    }
}
