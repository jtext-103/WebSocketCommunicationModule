using System.Collections.Concurrent;
using System;
using Jtext103.CFET2.Core.Event;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions
{
    internal sealed class AggregatedServerSubscription
    {
        internal volatile bool IsActive = true;

        public Token? LocalToken { get; set; }

        public IDisposable? LocalSubscription { get; set; }

        public EventFilter LocalFilter { get; init; } = new();

        public ConcurrentDictionary<RemoteSubscriptionKey, RemoteSubscriber> Subscribers { get; } = new();

        // Replaced under the manager lock, never mutated after publication. Queue items
        // retain this version so later subscriptions cannot inherit earlier events.
        internal RemoteSubscriber[] SubscriberSnapshot { get; private set; } = [];

        internal void RefreshSubscriberSnapshot() => SubscriberSnapshot = Subscribers.Values.ToArray();
    }
}
