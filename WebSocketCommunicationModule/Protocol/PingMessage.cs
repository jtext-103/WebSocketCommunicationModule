namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public sealed class PingMessage : HeartbeatMessage
    {
        public PingMessage()
            : base(WebSocketProtocolConstants.Ping)
        {
        }
    }
}
