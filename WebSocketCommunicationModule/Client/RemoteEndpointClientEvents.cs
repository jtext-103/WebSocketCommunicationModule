namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed class ConnectionStatusEventArgs
    {
        public string EventType { get; init; } = string.Empty;

        public string Reason { get; init; } = string.Empty;

        public int RetryCount { get; init; }
    }
}
