namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public sealed class PongMessage : HeartbeatMessage
    {
        public PongMessage()
            : base(WebSocketProtocolConstants.Pong)
        {
        }
    }
}
