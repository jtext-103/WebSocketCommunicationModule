using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed class RemoteEndpointSubscriptionStore
    {
        private readonly RemoteEndpointKey endpoint;
        private readonly object syncRoot = new();
        private readonly Dictionary<Guid, RemoteSubscriptionEntry> subscriptions = new();
        private readonly HashSet<Guid> pendingUnsubscribeTokenIds = new();

        public RemoteEndpointSubscriptionStore(RemoteEndpointKey endpoint)
        {
            this.endpoint = endpoint;
        }

        public int Count
        {
            get
            {
                lock (syncRoot)
                {
                    return subscriptions.Count;
                }
            }
        }

        public bool IsIdle => Count == 0;

        public bool IsEffectivelyIdle
        {
            get
            {
                lock (syncRoot)
                {
                    return subscriptions.Count == 0 ||
                        subscriptions.Values.All(entry =>
                            entry.State == SubscriptionState.Failed ||
                            pendingUnsubscribeTokenIds.Contains(entry.TokenId));
                }
            }
        }

        public void RemovePendingUnsubscribe(Guid tokenId)
        {
            lock (syncRoot)
            {
                pendingUnsubscribeTokenIds.Remove(tokenId);
            }
        }

        public void MarkPendingUnsubscribeIfContains(Guid tokenId)
        {
            lock (syncRoot)
            {
                if (subscriptions.ContainsKey(tokenId))
                {
                    pendingUnsubscribeTokenIds.Add(tokenId);
                }
            }
        }

        public void AddOrUpdate(Guid masterTokenId, int revision, EventFilter remoteFilter)
        {
            lock (syncRoot)
            {
                if (subscriptions.TryGetValue(masterTokenId, out var existing) &&
                    revision < existing.Revision)
                {
                    return;
                }

                pendingUnsubscribeTokenIds.Remove(masterTokenId);
                subscriptions[masterTokenId] = new RemoteSubscriptionEntry
                {
                    TokenId = masterTokenId,
                    Endpoint = endpoint,
                    Filter = remoteFilter,
                    Revision = revision,
                    State = SubscriptionState.Pending,
                    CreatedAtUtc = existing?.CreatedAtUtc ?? DateTime.UtcNow,
                    LastAcceptedRevision = existing?.LastAcceptedRevision,
                };
            }
        }

        public void AddOrUpdate(Guid masterTokenId, EventFilter remoteFilter)
        {
            AddOrUpdate(masterTokenId, 0, remoteFilter);
        }

        public bool Remove(Guid masterTokenId)
        {
            lock (syncRoot)
            {
                pendingUnsubscribeTokenIds.Remove(masterTokenId);
                return subscriptions.Remove(masterTokenId);
            }
        }

        public RemoteSubscriptionEntry? Get(Guid masterTokenId)
        {
            lock (syncRoot)
            {
                return subscriptions.TryGetValue(masterTokenId, out var entry) ? entry : null;
            }
        }

        public IReadOnlyList<RemoteSubscriptionEntry> Snapshot()
        {
            lock (syncRoot)
            {
                return subscriptions.Values.ToArray();
            }
        }

        public bool Contains(Guid masterTokenId)
        {
            lock (syncRoot)
            {
                return subscriptions.ContainsKey(masterTokenId);
            }
        }

        public bool TryMarkSubscribeSending(Guid masterTokenId)
        {
            lock (syncRoot)
            {
                if (!subscriptions.TryGetValue(masterTokenId, out var current) ||
                    current.State != SubscriptionState.Pending ||
                    current.SubscribeSentAtUtc != null ||
                    current.SubscribeSendInProgress)
                {
                    return false;
                }

                current.SubscribeSendInProgress = true;
                return true;
            }
        }

        public void MarkSubscribeSent(Guid masterTokenId, DateTime sentAtUtc)
        {
            lock (syncRoot)
            {
                if (subscriptions.TryGetValue(masterTokenId, out var current) &&
                    current.State == SubscriptionState.Pending)
                {
                    current.SubscribeSentAtUtc = sentAtUtc;
                    current.SubscribeSendInProgress = false;
                }
            }
        }

        public void MarkSubscribeSendFailed(Guid masterTokenId, string reason)
        {
            lock (syncRoot)
            {
                if (subscriptions.TryGetValue(masterTokenId, out var current))
                {
                    current.LastError = reason;
                    current.SubscribeSentAtUtc = null;
                    current.SubscribeSendInProgress = false;
                }
            }
        }

        public bool TryPrepareSubscribeRetry(
            Guid masterTokenId,
            int maxRetryCount,
            out EventFilter? filter,
            out int revision)
        {
            lock (syncRoot)
            {
                filter = null;
                revision = 0;
                if (!subscriptions.TryGetValue(masterTokenId, out var current) ||
                    current.State != SubscriptionState.Pending ||
                    current.SubscribeSentAtUtc == null ||
                    current.SubscribeSendInProgress ||
                    current.SubscribeRetryCount >= Math.Max(0, maxRetryCount))
                {
                    return false;
                }

                current.SubscribeRetryCount++;
                current.SubscribeSendInProgress = true;
                filter = current.Filter;
                revision = current.Revision;
                return true;
            }
        }

        public bool TryPrepareSubscribeRetry(
            Guid masterTokenId,
            int maxRetryCount,
            out EventFilter? filter)
        {
            return TryPrepareSubscribeRetry(masterTokenId, maxRetryCount, out filter, out _);
        }

        public IReadOnlyList<Guid> GetSubscribeAckTimeouts(DateTime nowUtc, TimeSpan timeout)
        {
            lock (syncRoot)
            {
                return subscriptions.Values
                    .Where(entry =>
                        entry.State == SubscriptionState.Pending &&
                        entry.SubscribeSentAtUtc.HasValue &&
                        nowUtc - entry.SubscribeSentAtUtc.Value >= timeout)
                    .Select(entry => entry.TokenId)
                    .ToArray();
            }
        }

        public bool HasExhaustedSubscribeRetries(Guid masterTokenId, int maxRetryCount)
        {
            lock (syncRoot)
            {
                return subscriptions.TryGetValue(masterTokenId, out var current) &&
                    current.State == SubscriptionState.Pending &&
                    current.SubscribeSentAtUtc.HasValue &&
                    current.SubscribeRetryCount >= Math.Max(0, maxRetryCount);
            }
        }

        public void MarkSubscribeAck(SubscribeAckMessage message)
        {
            lock (syncRoot)
            {
                if (!subscriptions.TryGetValue(message.TokenId, out var entry))
                {
                    return;
                }

                if (message.Revision != entry.Revision)
                {
                    return;
                }

                if (message.Accepted)
                {
                    entry.State = SubscriptionState.Active;
                    entry.LastAcceptedRevision = message.Revision;
                    entry.ActivatedAtUtc = DateTime.UtcNow;
                    entry.LastError = null;
                    entry.SubscribeSentAtUtc = null;
                    entry.SubscribeSendInProgress = false;
                    return;
                }

                entry.State = SubscriptionState.Failed;
                entry.SubscribeSendInProgress = false;
                entry.LastError = string.IsNullOrWhiteSpace(message.Reason)
                    ? "Subscribe rejected."
                    : message.Reason;
            }
        }

        public void MarkError(ErrorMessage message)
        {
            if (message.TokenId == null)
            {
                return;
            }

            lock (syncRoot)
            {
                if (subscriptions.TryGetValue(message.TokenId.Value, out var entry))
                {
                    entry.State = SubscriptionState.Failed;
                    entry.SubscribeSendInProgress = false;
                    entry.LastError = string.IsNullOrWhiteSpace(message.Reason)
                        ? message.Code
                        : message.Reason;
                }
            }
        }

        public void ResetForReconnect()
        {
            lock (syncRoot)
            {
                foreach (var entry in subscriptions.Values)
                {
                    if (entry.State == SubscriptionState.Active || entry.State == SubscriptionState.Pending)
                    {
                        entry.State = SubscriptionState.Pending;
                        entry.ActivatedAtUtc = null;
                        entry.SubscribeSentAtUtc = null;
                        entry.SubscribeRetryCount = 0;
                        entry.SubscribeSendInProgress = false;
                    }
                }
            }
        }

        public void MarkActiveOrPendingFailed(string reason)
        {
            lock (syncRoot)
            {
                foreach (var entry in subscriptions.Values)
                {
                    if (entry.State == SubscriptionState.Pending || entry.State == SubscriptionState.Active)
                    {
                        entry.State = SubscriptionState.Failed;
                        entry.LastError = reason;
                    }
                }
            }
        }

        public void Clear()
        {
            lock (syncRoot)
            {
                subscriptions.Clear();
                pendingUnsubscribeTokenIds.Clear();
            }
        }
    }
}
