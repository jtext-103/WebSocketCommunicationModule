using System.Net.WebSockets;
using Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server
{
    public sealed partial class WebSocketSession
    {
        internal WebSocketServerSessionStatus GetDiagnosticStatus()
        {
            return new WebSocketServerSessionStatus
            {
                SessionId = Id,
                State = Volatile.Read(ref disposed) != 0 ? "Disposed" : socket.State.ToString(),
                RemoteAddress = remoteAddress,
                CreatedAtUtc = createdAtUtc,
                HandshakeCompletedAtUtc = handshakeCompletedAtUtc,
                LastReceivedAtUtc = lastReceivedAtUtc == default ? createdAtUtc : lastReceivedAtUtc,
                RemoteSubscriptionCount = remoteTokens.Count,
            };
        }
    }
}
