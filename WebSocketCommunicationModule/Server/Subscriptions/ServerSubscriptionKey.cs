namespace Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions
{
    /// <summary>
    /// ignore case for source and event type since server subscription is not expected to be large in scale and this can improve usability.
    /// </summary>
    internal readonly record struct ServerSubscriptionKey : IEquatable<ServerSubscriptionKey>
    {
        public string Source { get; }

        public string EventType { get; }
        public ServerSubscriptionKey(string source, string eventType)
        {
            Source = source;
            EventType = eventType;
        }

        public bool Equals(ServerSubscriptionKey other)
        {
            return StringComparer.OrdinalIgnoreCase.Equals(Source, other.Source)
                && StringComparer.OrdinalIgnoreCase.Equals(EventType, other.EventType);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                StringComparer.OrdinalIgnoreCase.GetHashCode(Source),
                StringComparer.OrdinalIgnoreCase.GetHashCode(EventType));
        }
    }
}
