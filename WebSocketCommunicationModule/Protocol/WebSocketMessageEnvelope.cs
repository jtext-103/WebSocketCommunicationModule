namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public abstract class WebSocketMessageEnvelope
    {
        protected WebSocketMessageEnvelope(string messageType)
        {
            MessageType = messageType;
        }

        public string MessageType { get; init; }

        public string Protocol { get; init; } = WebSocketProtocolConstants.Protocol;

        public int Version { get; init; } = WebSocketProtocolConstants.Version;
    }
}
