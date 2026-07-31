using System;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Connections
{
    internal sealed class LocalSubscriptionBinding
    {
        public Guid LocalTokenId { get; init; }

        public RemoteEndpointKey Endpoint { get; init; }

        public ClientSubscriptionGroupKey[] GroupKeys { get; init; } = [];

        public Guid[] MasterTokenIds { get; init; } = [];
    }
}
