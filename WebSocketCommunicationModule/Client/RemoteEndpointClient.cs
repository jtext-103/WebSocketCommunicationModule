using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed partial class RemoteEndpointClient : IDisposable
    {
        private readonly WebSocketCommunicationOptions options;
        private readonly RemoteEndpointSubscriptionStore subscriptionStore;
        private readonly RemoteEndpointConnection connection;
        private readonly RemoteEndpointActionProcessor actionProcessor;
        private Timer? idleReleaseTimer;
        private volatile bool disposed;

        public RemoteEndpointClient(
            RemoteEndpointKey endpoint,
            WebSocketCommunicationOptions options,
            ICfet2Logger logger)
        {
            Endpoint = endpoint;
            this.options = options ?? throw new ArgumentNullException(nameof(options));
            if (logger == null)
            {
                throw new ArgumentNullException(nameof(logger));
            }

            subscriptionStore = new RemoteEndpointSubscriptionStore(endpoint);
            connection = new RemoteEndpointConnection(
                endpoint,
                this.options,
                logger,
                subscriptionStore,
                RaiseRemoteEventReceived,
                RaiseConnectionStatusChanged);
            actionProcessor = new RemoteEndpointActionProcessor(
                logger,
                () => disposed,
                (tokenId, revision, filter) => AddOrUpdateSubscription(tokenId, revision, filter),
                RemoveSubscription,
                EnsureConnectedAsync,
                SendUnsubscribeAsync,
                subscriptionStore,
                endpoint,
                options.ActionQueueWarningThreshold);
        }

        public RemoteEndpointKey Endpoint { get; }

        public RemoteConnectionState State => disposed
            ? RemoteConnectionState.Disposed
            : connection.State;

        public int SubscriptionCount => subscriptionStore.Count;

        public bool IsIdle => subscriptionStore.IsIdle;

        internal TimeSpan EffectiveHeartbeatInterval => connection.EffectiveHeartbeatInterval;

        internal TimeSpan EffectiveHeartbeatTimeout => connection.EffectiveHeartbeatTimeout;

        public event Action<RemoteEndpointClient, RemoteEventMessage>? RemoteEventReceived;

        public event Action<RemoteEndpointClient, ConnectionStatusEventArgs>? ConnectionStatusChanged;

        public event Action<RemoteEndpointClient>? Idle;

        public void EnqueueSubscribe(Guid masterTokenId, int revision, EventFilter remoteFilter)
        {
            actionProcessor.EnqueueSubscribe(masterTokenId, revision, remoteFilter);
        }

        public void EnqueueSubscribe(Guid masterTokenId, EventFilter remoteFilter)
        {
            EnqueueSubscribe(masterTokenId, 0, remoteFilter);
        }

        public void EnqueueUnsubscribe(Guid masterTokenId)
        {
            actionProcessor.EnqueueUnsubscribe(masterTokenId);
        }

        public void AddOrUpdateSubscription(Guid masterTokenId, int revision, EventFilter remoteFilter)
        {
            subscriptionStore.AddOrUpdate(masterTokenId, revision, remoteFilter);
            idleReleaseTimer?.Dispose();
            idleReleaseTimer = null;
        }

        public void AddOrUpdateSubscription(Guid masterTokenId, EventFilter remoteFilter)
        {
            AddOrUpdateSubscription(masterTokenId, 0, remoteFilter);
        }

        public bool RemoveSubscription(Guid masterTokenId)
        {
            return subscriptionStore.Remove(masterTokenId);
        }

        public RemoteSubscriptionEntry? GetSubscription(Guid masterTokenId)
        {
            return subscriptionStore.Get(masterTokenId);
        }

        public IReadOnlyList<RemoteSubscriptionEntry> SnapshotSubscriptions()
        {
            return subscriptionStore.Snapshot();
        }

        public Task EnsureConnectedAsync()
        {
            return connection.EnsureConnectedAsync();
        }

        public async Task SendUnsubscribeAsync(Guid masterTokenId)
        {
            await connection.SendUnsubscribeAsync(masterTokenId).ConfigureAwait(false);
            ScheduleIdleReleaseIfStillIdle();
        }

        public void Dispose()
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
            idleReleaseTimer?.Dispose();
            connection.Dispose();
            actionProcessor.Dispose();
            subscriptionStore.Clear();
        }

        private void ScheduleIdleReleaseIfStillIdle()
        {
            idleReleaseTimer?.Dispose();
            idleReleaseTimer = null;

            if (!IsIdle)
            {
                return;
            }

            if (options.ReleaseConnectionImmediatelyWhenIdle || options.IdleReleaseDelay <= TimeSpan.Zero)
            {
                RaiseIdleIfStillIdle();
                return;
            }

            idleReleaseTimer = new Timer(
                _ => RaiseIdleIfStillIdle(),
                null,
                options.IdleReleaseDelay,
                Timeout.InfiniteTimeSpan);
        }

        private void RaiseRemoteEventReceived(RemoteEventMessage message)
        {
            if (disposed)
            {
                return;
            }

            RemoteEventReceived?.Invoke(this, message);
        }

        private void RaiseConnectionStatusChanged(string eventType, string reason, int retryCount)
        {
            if (disposed)
            {
                return;
            }

            ConnectionStatusChanged?.Invoke(this, new ConnectionStatusEventArgs
            {
                EventType = eventType,
                Reason = reason,
                RetryCount = retryCount,
            });
        }

        private void RaiseIdleIfStillIdle()
        {
            if (disposed || !IsIdle)
            {
                return;
            }

            Idle?.Invoke(this);
        }
    }
}
