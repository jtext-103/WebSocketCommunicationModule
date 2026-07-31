using System;
using Jtext103.CFET2.Core.Event;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public sealed class SubscribeMessage : WebSocketMessageEnvelope
    {
        public SubscribeMessage()
            : base(WebSocketProtocolConstants.Subscribe)
        {
        }

        public Guid TokenId { get; init; }

        public int Revision { get; init; }

        public EventFilter? Filter { get; init; }
    }
}
