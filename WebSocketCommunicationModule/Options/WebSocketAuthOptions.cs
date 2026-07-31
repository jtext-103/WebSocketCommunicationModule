namespace Jtext103.CFET2.WebSocketCommunicationModule.Options
{
    // Reserved for future WebSocket authentication support. The current server
    // intentionally accepts anonymous connections only.
    public sealed class WebSocketAuthOptions
    {
        public bool Enabled { get; set; }

        public string? Scheme { get; set; }

        public string? Token { get; set; }
    }
}
