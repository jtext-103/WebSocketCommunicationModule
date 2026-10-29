using System;
using Jtext103.CFET2.Core.Event;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public sealed class RemoteEventMessage : WebSocketMessageEnvelope
    {
        public RemoteEventMessage()
            : base(WebSocketProtocolConstants.Event)
        {
        }

        public Guid? TokenId { get; init; }

        public int PerformanceLevel { get; init; }
        public bool NoEvent { get; init; }

        public string? Source { get; init; }

        public string? EventType { get; init; }

        public object? Value { get; init; }

        /// <summary>Source change time for a lightweight event; absent in legacy messages.</summary>
        public DateTimeOffset? Timestamp { get; init; }

        public EventArg? EventArg { get; init; }
    }
}
