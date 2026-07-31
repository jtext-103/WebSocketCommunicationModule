using System;
using System.Collections.Concurrent;
using Jtext103.CFET2.Core.Event;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Connections
{
    internal sealed class ClientSubscriptionGroup
    {
        public Guid MasterTokenId { get; init; }

        public ClientSubscriptionGroupKey Key { get; init; }

        public RemoteEndpointKey Endpoint { get => Key.Endpoint; }

        public EventFilter RemoteFilter { get; set; } = new();

        public ConcurrentDictionary<Guid, int> LocalTokenIds { get; } = new();

        public int EffectivePerformanceLevel { get; set; }

        public int Revision { get; set; }

        public DateTime CreatedAtUtc { get; init; }
    }
}
