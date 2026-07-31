using System.Collections.Concurrent;
using Jtext103.CFET2.Core.Event;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions
{
    internal sealed class AggregatedServerSubscription
    {
        public Token? LocalToken { get; set; }

        public EventFilter LocalFilter { get; init; } = new();

        public ConcurrentDictionary<RemoteSubscriptionKey, RemoteSubscriber> Subscribers { get; } = new();
    }
}
