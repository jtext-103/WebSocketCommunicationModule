using System;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public sealed class ErrorMessage : WebSocketMessageEnvelope
    {
        public ErrorMessage()
            : base(WebSocketProtocolConstants.Error)
        {
        }

        public Guid? TokenId { get; init; }

        public string? Code { get; init; }

        public string? Reason { get; init; }
    }
}
