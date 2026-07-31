namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public sealed class HelloMessage : WebSocketMessageEnvelope
    {
        public HelloMessage()
            : base(WebSocketProtocolConstants.Hello)
        {
        }

        public string? ClientId { get; init; }

        public string? AuthScheme { get; init; }

        public string? AuthToken { get; init; }
    }
}
