namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public static class WebSocketProtocolConstants
    {
        public const string EndpointPath = "/cfet2/event/v2";

        public const string SubProtocol = "cfet2-event.v2";

        public const string Protocol = "cfet2.websocket.event";

        public const int Version = 2;

        public const string Hello = "hello";

        public const string HelloAck = "helloAck";

        public const string Subscribe = "subscribe";

        public const string SubscribeAck = "subscribeAck";

        public const string Unsubscribe = "unsubscribe";

        public const string Event = "event";

        public const string Ping = "ping";

        public const string Pong = "pong";

        public const string Heartbeat = "heartbeat";

        public const string Error = "error";
    }
}
