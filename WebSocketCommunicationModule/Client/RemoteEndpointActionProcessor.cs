using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using System.Threading.Channels;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed partial class RemoteEndpointActionProcessor : IDisposable
    {

        private readonly Func<bool> isDisposed;
        private readonly Action<Guid, int, EventFilter> addOrUpdateSubscription;
        private readonly Func<Guid, bool> removeSubscription;
        private readonly Func<Task> ensureConnectedAsync;
        private readonly Func<Guid, Task> sendUnsubscribeAsync;
        private readonly RemoteEndpointSubscriptionStore subscriptionStore;
        private readonly RemoteEndpointKey endpoint;
        private readonly int actionQueueWarningThreshold;
        private readonly CancellationTokenSource stopCts = new();
        private readonly Channel<RemoteEndpointAction> actionQueue = Channel.CreateUnbounded<RemoteEndpointAction>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
            });
        private readonly Task actionLoopTask;
        private int queuedActionCount;
        private int queueWarningActive;
        private volatile bool disposed;

        public RemoteEndpointActionProcessor(

            Func<bool> isDisposed,
            Action<Guid, int, EventFilter> addOrUpdateSubscription,
            Func<Guid, bool> removeSubscription,
            Func<Task> ensureConnectedAsync,
            Func<Guid, Task> sendUnsubscribeAsync,
            RemoteEndpointSubscriptionStore subscriptionStore,
            RemoteEndpointKey? endpoint = null,
            int actionQueueWarningThreshold = int.MaxValue)
        {

            this.isDisposed = isDisposed;
            this.addOrUpdateSubscription = addOrUpdateSubscription;
            this.removeSubscription = removeSubscription;
            this.ensureConnectedAsync = ensureConnectedAsync;
            this.sendUnsubscribeAsync = sendUnsubscribeAsync;
            this.subscriptionStore = subscriptionStore;
            this.endpoint = endpoint ?? default;
            this.actionQueueWarningThreshold = actionQueueWarningThreshold;
            actionLoopTask = ProcessActionsAsync();
            _ = RemoteEndpointBackgroundTask.Observe(actionLoopTask);
        }

        public RemoteEndpointActionProcessor(

            Func<bool> isDisposed,
            Action<Guid, EventFilter> addOrUpdateSubscription,
            Func<Guid, bool> removeSubscription,
            Func<Task> ensureConnectedAsync,
            Func<Guid, Task> sendUnsubscribeAsync,
            RemoteEndpointSubscriptionStore subscriptionStore,
            RemoteEndpointKey? endpoint = null,
            int actionQueueWarningThreshold = int.MaxValue)
            : this(

                isDisposed,
                (tokenId, _, filter) => addOrUpdateSubscription(tokenId, filter),
                removeSubscription,
                ensureConnectedAsync,
                sendUnsubscribeAsync,
                subscriptionStore,
                endpoint,
                actionQueueWarningThreshold)
        {
        }

        public void EnqueueSubscribe(Guid masterTokenId, int revision, EventFilter remoteFilter)
        {
            if (disposed || isDisposed())
            {
                WebSocketLog.Logger.Warn($"WebSocket subscribe action enqueue failed: {masterTokenId}");
                return;
            }

            subscriptionStore.RemovePendingUnsubscribe(masterTokenId);

            if (!TryEnqueue(new RemoteEndpointAction(RemoteEndpointActionKind.Subscribe, masterTokenId, revision, remoteFilter)))
            {
                WebSocketLog.Logger.Warn($"WebSocket subscribe action enqueue failed: {masterTokenId}");
            }
        }

        public void EnqueueSubscribe(Guid masterTokenId, EventFilter remoteFilter)
        {
            EnqueueSubscribe(masterTokenId, 0, remoteFilter);
        }

        public void EnqueueUnsubscribe(Guid masterTokenId)
        {
            if (disposed || isDisposed())
            {
                WebSocketLog.Logger.Warn($"WebSocket unsubscribe action enqueue failed: {masterTokenId}");
                return;
            }

            removeSubscription(masterTokenId);

            if (!TryEnqueue(new RemoteEndpointAction(RemoteEndpointActionKind.Unsubscribe, masterTokenId, 0, null)))
            {
                WebSocketLog.Logger.Warn($"WebSocket unsubscribe action enqueue failed: {masterTokenId}");
            }
        }

        public void Dispose()
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
            stopCts.Cancel();
            actionQueue.Writer.TryComplete();
            WaitForActionLoopToStop();
            stopCts.Dispose();
        }

        private async Task ProcessActionsAsync()
        {
            try
            {
                await foreach (var action in actionQueue.Reader.ReadAllAsync(stopCts.Token).ConfigureAwait(false))
                {
                    OnActionDequeued();
                    try
                    {
                        switch (action.Kind)
                        {
                            case RemoteEndpointActionKind.Subscribe:
                                addOrUpdateSubscription(action.MasterTokenId, action.Revision, action.RemoteFilter!);
                                await ensureConnectedAsync().ConfigureAwait(false);
                                break;
                            case RemoteEndpointActionKind.Unsubscribe:
                                removeSubscription(action.MasterTokenId);
                                await sendUnsubscribeAsync(action.MasterTokenId).ConfigureAwait(false);
                                break;
                        }
                    }
                    catch
                    {
                        WebSocketLog.Logger.Error($"{action} excute error! skip this action!");
                    }
                }
            }
            catch (OperationCanceledException) when (disposed || stopCts.IsCancellationRequested)
            {
            }
            finally
            {
            }
        }

        private bool TryEnqueue(RemoteEndpointAction action)
        {
            var count = Interlocked.Increment(ref queuedActionCount);
            if (!actionQueue.Writer.TryWrite(action))
            {
                Interlocked.Decrement(ref queuedActionCount);
                return false;
            }

            if (actionQueueWarningThreshold > 0 &&
                count > actionQueueWarningThreshold &&
                Interlocked.CompareExchange(ref queueWarningActive, 1, 0) == 0)
            {
                WebSocketLog.Logger.Error(
                    $"WebSocket action queue warning: endpoint={endpoint}, " +
                    $"queueLength={count}, threshold={actionQueueWarningThreshold}.");
            }

            return true;
        }

        private void OnActionDequeued()
        {
            var count = Interlocked.Decrement(ref queuedActionCount);
            if (actionQueueWarningThreshold > 0 &&
                count < actionQueueWarningThreshold / 2.0)
            {
                Interlocked.Exchange(ref queueWarningActive, 0);
            }
        }

        private void WaitForActionLoopToStop()
        {
            try
            {
                actionLoopTask.Wait(TimeSpan.FromSeconds(1));
            }
            catch
            {
            }
        }
    }
}
