namespace Jtext103.CFET2.WebSocketCommunicationModule.Connections
{
    /// <summary>
    /// Preserve regex text semantics while sharing case variants of literal conditions.
    /// </summary>
    internal readonly record struct ClientSubscriptionGroupKey : IEquatable<ClientSubscriptionGroupKey>
    {
        public RemoteEndpointKey Endpoint { get; }

        public string Source { get; }

        public string EventType { get; }
        public bool IsResourceSubscription { get; }
        public ClientSubscriptionGroupKey(RemoteEndpointKey endpoint, string source, string eventType, bool isResourceSubscription = false)
        {
            Endpoint = endpoint;
            Source = source;
            EventType = eventType;
            IsResourceSubscription = isResourceSubscription;
        }
        public bool Equals(ClientSubscriptionGroupKey other)
        {
            return Endpoint.Equals(other.Endpoint)
                && SubscriptionExpression.Equal(Source, other.Source)
                && SubscriptionExpression.Equal(EventType, other.EventType)
                && IsResourceSubscription == other.IsResourceSubscription;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                Endpoint,
                SubscriptionExpression.Hash(Source),
                SubscriptionExpression.Hash(EventType), IsResourceSubscription);
        }
    }
}
