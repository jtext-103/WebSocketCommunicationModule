using System;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public sealed class UnsubscribeMessage : WebSocketMessageEnvelope
    {
        public UnsubscribeMessage()
            : base(WebSocketProtocolConstants.Unsubscribe)
        {
        }

        public Guid TokenId { get; init; }
    }
}
