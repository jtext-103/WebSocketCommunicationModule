using System;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public sealed class SubscribeAckMessage : WebSocketMessageEnvelope
    {
        public SubscribeAckMessage()
            : base(WebSocketProtocolConstants.SubscribeAck)
        {
        }

        public Guid TokenId { get; init; }

        public int Revision { get; init; }

        public bool Accepted { get; init; }

        public string? Reason { get; init; }
        public string[]? SkippedResources { get; init; }
        public bool Skipped { get; init; }
    }
}
