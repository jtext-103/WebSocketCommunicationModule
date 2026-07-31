using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed class ClientSubscriptionPlan
    {
        public IReadOnlyList<ClientRemoteAction> Actions { get; init; } = [];

        public bool IsEmpty => Actions.Count == 0;
    }

    internal sealed class ClientRemoteAction
    {
        public RemoteEndpointActionKind Kind { get; init; }

        public RemoteEndpointKey Endpoint { get; init; }

        public Guid MasterTokenId { get; init; }

        public int Revision { get; init; }

        public EventFilter? RemoteFilter { get; init; }
    }
}
