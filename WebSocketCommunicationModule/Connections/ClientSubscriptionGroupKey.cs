namespace Jtext103.CFET2.WebSocketCommunicationModule.Connections
{
    /// <summary>
    /// ignore case for source and event type since client subscription is not expected to be large in scale and this can improve usability.
    /// </summary>
    internal readonly record struct ClientSubscriptionGroupKey : IEquatable<ClientSubscriptionGroupKey>
    {
        public RemoteEndpointKey Endpoint { get; }

        public string Source { get; }

        public string EventType { get; }
        public ClientSubscriptionGroupKey(RemoteEndpointKey endpoint, string source, string eventType)
        {
            Endpoint = endpoint;
            Source = source;
            EventType = eventType;
        }
        public bool Equals(ClientSubscriptionGroupKey other)
        {
            return Endpoint.Equals(other.Endpoint)
                && StringComparer.OrdinalIgnoreCase.Equals(Source, other.Source)
                && StringComparer.OrdinalIgnoreCase.Equals(EventType, other.EventType);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                Endpoint,
                StringComparer.OrdinalIgnoreCase.GetHashCode(Source),
                StringComparer.OrdinalIgnoreCase.GetHashCode(EventType));
        }
    }
}
