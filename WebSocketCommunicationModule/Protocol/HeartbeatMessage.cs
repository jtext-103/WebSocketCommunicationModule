using System;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public class HeartbeatMessage : WebSocketMessageEnvelope
    {
        public HeartbeatMessage()
            : this(WebSocketProtocolConstants.Heartbeat)
        {
        }

        protected HeartbeatMessage(string messageType)
            : base(messageType)
        {
        }

        public DateTime TimestampUtc { get; init; } = DateTime.UtcNow;
    }
}
