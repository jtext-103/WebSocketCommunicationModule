using Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed partial class RemoteEndpointClient
    {
        internal WebSocketRemoteClientStatus GetDiagnosticStatus()
        {
            var subscriptions = subscriptionStore
                .Snapshot()
                .Select(CreateSubscriptionStatus)
                .OrderBy(status => status.Source, StringComparer.OrdinalIgnoreCase)
                .ThenBy(status => status.EventType, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            return new WebSocketRemoteClientStatus
            {
                Endpoint = Endpoint.NormalizedHost,
                State = State.ToString(),
                ConnectedAtUtc = connection.ConnectedAtUtc,
                LastReceivedAtUtc = connection.LastReceivedAtUtc,
                RetryCount = connection.RetryCount,
                LastError = connection.LastError,
                ActionQueueLength = actionProcessor.QueueLength,
                SubscriptionCount = subscriptions.Length,
                PendingSubscriptionCount = subscriptions.Count(status => status.State == SubscriptionState.Pending.ToString()),
                ActiveSubscriptionCount = subscriptions.Count(status => status.State == SubscriptionState.Active.ToString()),
                FailedSubscriptionCount = subscriptions.Count(status => status.State == SubscriptionState.Failed.ToString()),
                Subscriptions = subscriptions,
            };
        }

        private static WebSocketRemoteSubscriptionStatus CreateSubscriptionStatus(RemoteSubscriptionEntry entry)
        {
            var rule = entry.Filter.SourcesAndTypes.FirstOrDefault();
            return new WebSocketRemoteSubscriptionStatus
            {
                TokenId = entry.TokenId,
                Source = rule?.Source ?? string.Empty,
                EventType = rule?.EventType ?? string.Empty,
                PerformanceLevel = entry.Filter.PerformanceLevel,
                Revision = entry.Revision,
                LastAcceptedRevision = entry.LastAcceptedRevision,
                State = entry.State.ToString(),
                CreatedAtUtc = entry.CreatedAtUtc,
                ActivatedAtUtc = entry.ActivatedAtUtc,
                SubscribeSentAtUtc = entry.SubscribeSentAtUtc,
                SubscribeRetryCount = entry.SubscribeRetryCount,
                LastError = entry.LastError,
                SkippedResources = entry.SkippedResources.ToArray(),
            };
        }
    }
}
