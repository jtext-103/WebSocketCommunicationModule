using System;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    public sealed class RemoteSubscriptionEntry
    {
        public Guid TokenId { get; init; }

        public RemoteEndpointKey Endpoint { get; init; }

        public EventFilter Filter { get; init; } = new();

        public int Revision { get; init; }

        public int? LastAcceptedRevision { get; set; }

        public SubscriptionState State { get; set; }

        public DateTime CreatedAtUtc { get; init; }

        public DateTime? ActivatedAtUtc { get; set; }

        public DateTime? SubscribeSentAtUtc { get; set; }

        public int SubscribeRetryCount { get; set; }

        internal bool SubscribeSendInProgress { get; set; }

        public string[] SkippedResources { get; set; } = Array.Empty<string>();

        public string? LastError { get; set; }
    }
}
