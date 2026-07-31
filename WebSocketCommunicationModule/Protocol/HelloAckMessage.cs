namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public sealed class HelloAckMessage : WebSocketMessageEnvelope
    {
        public HelloAckMessage()
            : base(WebSocketProtocolConstants.HelloAck)
        {
        }

        public bool Accepted { get; init; }

        public string? Reason { get; init; }

        public int HeartbeatIntervalMilliseconds { get; init; }

        public int? HeartbeatTimeoutMilliseconds { get; init; }
    }
}
