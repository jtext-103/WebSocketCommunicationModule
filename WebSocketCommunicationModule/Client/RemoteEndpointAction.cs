using Jtext103.CFET2.Core.Event;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal enum RemoteEndpointActionKind
    {
        Subscribe,
        Unsubscribe,
    }

    internal readonly record struct RemoteEndpointAction(
        RemoteEndpointActionKind Kind,
        Guid MasterTokenId,
        int Revision,
        EventFilter? RemoteFilter)
    {
        public override string ToString()
        {
            return $"RemoteEndpointAction(Kind={Kind}, MasterTokenId={MasterTokenId:N}, RemoteFilter={RemoteFilter?.ToString() ?? "null"})";
        }
    }
}
