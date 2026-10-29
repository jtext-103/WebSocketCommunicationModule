using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
namespace Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions
{
    /// <summary>
    /// Preserve regex text semantics while sharing case variants of literal conditions.
    /// </summary>
    internal readonly record struct ServerSubscriptionKey : IEquatable<ServerSubscriptionKey>
    {
        public string Source { get; }

        public string EventType { get; }
        public bool IsResourceSubscription { get; }
        public ServerSubscriptionKey(string source, string eventType, bool isResourceSubscription = false)
        {
            Source = source;
            EventType = eventType;
            IsResourceSubscription = isResourceSubscription;
        }

        public bool Equals(ServerSubscriptionKey other)
        {
            return SubscriptionExpression.Equal(Source, other.Source)
                && SubscriptionExpression.Equal(EventType, other.EventType)
                && IsResourceSubscription == other.IsResourceSubscription;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                SubscriptionExpression.Hash(Source),
                SubscriptionExpression.Hash(EventType), IsResourceSubscription);
        }
    }
}
