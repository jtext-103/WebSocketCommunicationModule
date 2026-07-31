using System;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions
{
    internal sealed class RemoteSubscriber
    {
        public RemoteSubscriber(
            Guid sessionId,
            Guid remoteTokenId,
            WebSocketSession session,
            string filterSignature,
            int performanceLevel,
            int revision)
        {
            SessionId = sessionId;
            RemoteTokenId = remoteTokenId;
            Session = session;
            FilterSignature = filterSignature;
            PerformanceLevel = performanceLevel;
            Revision = revision;
        }

        public Guid SessionId { get; }

        public Guid RemoteTokenId { get; }

        public WebSocketSession Session { get; }

        public string FilterSignature { get; set; }

        public int PerformanceLevel { get; set; }

        public int Revision { get; set; }

        public bool LastAccepted { get; set; } = true;

        public string? LastReason { get; set; }
    }
}
