using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Exceptions;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using System.Net.WebSockets;
using System.Threading.Channels;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed partial class RemoteEndpointConnection : IDisposable
    {
        private readonly RemoteEndpointKey endpoint;
        private readonly WebSocketCommunicationOptions options;

        private readonly RemoteEndpointSubscriptionStore subscriptionStore;
        private readonly Action<string, string, int> connectionStatusChanged;
        private readonly CancellationTokenSource lifetimeCts = new();
        private readonly SemaphoreSlim sendLock = new(1, 1);
        private readonly SemaphoreSlim connectLock = new(1, 1);
        private readonly SemaphoreSlim flushLock = new(1, 1);
        private readonly Channel<byte> refreshRequests = Channel.CreateBounded<byte>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });
        private readonly Task refreshTask;
        private readonly object reconnectLock = new();
        private readonly object connectionStateLock = new();
        private CancellationTokenSource? connectionCts;
        private readonly RemoteEndpointMessageHandler messageHandler;
        private ClientWebSocket? webSocket;
        private Task? receiveLoopTask;
        private Task? reconnectTask;
        private Task? heartbeatTask;
        private Task? subscribeAckTask;
        private DateTime? connectedAtUtc;
        private DateTime lastReceivedAtUtc;
        private string? lastError;
        private TimeSpan effectiveHeartbeatInterval;
        private TimeSpan effectiveHeartbeatTimeout;
        private long connectionGeneration;
        private int retryCount;
        private volatile bool disposed;

        public RemoteEndpointConnection(
            RemoteEndpointKey endpoint,
            WebSocketCommunicationOptions options,
            RemoteEndpointSubscriptionStore subscriptionStore,
            Action<RemoteEventMessage> remoteEventReceived,
            Action<string, string, int> connectionStatusChanged)
        {
            this.endpoint = endpoint;
            this.options = options;

            this.subscriptionStore = subscriptionStore;
            this.connectionStatusChanged = connectionStatusChanged;
            effectiveHeartbeatInterval = options.HeartbeatInterval;
            effectiveHeartbeatTimeout = options.HeartbeatTimeout;
            messageHandler = new RemoteEndpointMessageHandler(
                subscriptionStore,
                IsCurrentGeneration,
                (socket, message, timeout) => SendMessageAsync(socket, message, timeout),
                options.SendTimeout,
                value => lastReceivedAtUtc = value,
                remoteEventReceived,
                () => disposed,
                HandleConnectionFailure,
                HandleConnectionFailure);
            refreshTask = Task.Run(ProcessRefreshRequestsAsync);
            _ = RemoteEndpointBackgroundTask.Observe(refreshTask);
        }

        private volatile RemoteConnectionState state;
        public RemoteConnectionState State { get => state; private set => state = value; }

        internal TimeSpan EffectiveHeartbeatInterval => effectiveHeartbeatInterval;

        internal TimeSpan EffectiveHeartbeatTimeout => effectiveHeartbeatTimeout;

        internal DateTime? ConnectedAtUtc => connectedAtUtc;

        internal DateTime? LastReceivedAtUtc => lastReceivedAtUtc == default ? null : lastReceivedAtUtc;

        internal int RetryCount => Volatile.Read(ref retryCount);

        internal string? LastError => lastError;

        // The action reader applies ordered state changes without waiting for network IO
        // or resubscribe pacing. One retained signal covers changes made during a flush.
        internal Task RequestRefreshAsync()
        {
            if (!disposed) refreshRequests.Writer.TryWrite(0);
            return Task.CompletedTask;
        }

        private async Task ProcessRefreshRequestsAsync()
        {
            try
            {
                await foreach (var _ in refreshRequests.Reader.ReadAllAsync(lifetimeCts.Token).ConfigureAwait(false))
                    await EnsureConnectedAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (lifetimeCts.IsCancellationRequested) { }
        }

        public async Task EnsureConnectedAsync()
        {
            // double-check the conditions before attempting to connect, in case they have changed while waiting for the lock
            if (disposed || State == RemoteConnectionState.Disposed || subscriptionStore.IsEffectivelyIdle)
            {
                return;
            }

            Exception? failure = null;
            var notifyConnected = false;
            long generation = 0;
            await connectLock.WaitAsync(lifetimeCts.Token).ConfigureAwait(false);
            try
            {
                if (disposed || State == RemoteConnectionState.Disposed || subscriptionStore.IsEffectivelyIdle)
                {
                    return;
                }

                if (webSocket?.State != WebSocketState.Open || State != RemoteConnectionState.Connected)
                {
                    await ConnectAndHandshakeAsync().ConfigureAwait(false);
                    notifyConnected = State == RemoteConnectionState.Connected;
                }
                generation = Interlocked.Read(ref connectionGeneration);
            }
            catch (OperationCanceledException) when (disposed || lifetimeCts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                failure = ex;
                generation = Interlocked.Read(ref connectionGeneration);
            }
            finally
            {
                connectLock.Release();
            }
            if (!IsCurrentGeneration(generation)) return;
            if (failure != null) { HandleConnectionFailure(failure, generation); return; }
            try
            {
                if (notifyConnected && State == RemoteConnectionState.Connected)
                    connectionStatusChanged(ConnectionStatusEvent.Connected, "handshake completed", 0);
                await FlushSubscriptionsAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (disposed || lifetimeCts.IsCancellationRequested) { }
            catch (Exception ex)
            {
                HandleConnectionFailure(ex, generation);
            }
        }

        public async Task SendUnsubscribeAsync(Guid masterTokenId)
        {
            var socket = webSocket;
            var generation = Interlocked.Read(ref connectionGeneration);
            if (disposed ||
                socket?.State != WebSocketState.Open ||
                State != RemoteConnectionState.Connected)
            {
                return;
            }

            var maxRetryCount = Math.Max(0, options.MaxUnsubscribeRetryCount);
            for (var retryCount = 0; retryCount <= maxRetryCount; retryCount++)
            {
                try
                {
                    await SendMessageAsync(
                            socket,
                            new UnsubscribeMessage { TokenId = masterTokenId },
                            options.SendTimeout)
                        .ConfigureAwait(false);
                    return;
                }
                catch (Exception ex) when (ex is not OperationCanceledException ||
                                           !lifetimeCts.IsCancellationRequested)
                {
                    if (disposed ||
                        !IsCurrentGeneration(generation) ||
                        socket.State != WebSocketState.Open ||
                        State != RemoteConnectionState.Connected)
                    {
                        return;
                    }

                    if (retryCount >= maxRetryCount)
                    {
                        WebSocketLog.Logger.Warn(
                            $"WebSocket unsubscribe {masterTokenId} failed after {retryCount} retries: {ex.Message}");
                        socket.Abort();
                        HandleConnectionFailure(
                            new TransientConnectionException(
                                $"unsubscribe failed after {retryCount} retries",
                                ex), generation);
                        return;
                    }

                    await options.DelayProvider
                        .DelayAsync(options.UnsubscribeRetryInterval, lifetimeCts.Token)
                        .ConfigureAwait(false);
                }
            }
        }

        public void Dispose()
        {
            ClientWebSocket? socket;
            CancellationTokenSource? connectionStop;
            lock (connectionStateLock)
            {
                if (disposed) return;
                disposed = true;
                State = RemoteConnectionState.Disposed;
                Interlocked.Increment(ref connectionGeneration);
                socket = webSocket;
                webSocket = null;
                connectionStop = connectionCts;
                connectionCts = null;
            }
            refreshRequests.Writer.TryComplete();
            lifetimeCts.Cancel();
            connectionStop?.Cancel();
            connectionStop?.Dispose();
            socket?.Dispose();
            WaitForBackgroundTasksToStop();
        }

        private async Task ConnectAndHandshakeAsync()
        {
            ValidateConfiguration();
            var socket = new ClientWebSocket();
            long generation;
            ClientWebSocket? previousSocket;
            CancellationTokenSource? previousStop;
            lock (connectionStateLock)
            {
                if (disposed) { socket.Dispose(); return; }
                generation = Interlocked.Increment(ref connectionGeneration);
                previousSocket = webSocket;
                previousStop = connectionCts;
                webSocket = socket;
                connectionCts = CancellationTokenSource.CreateLinkedTokenSource(lifetimeCts.Token);
                State = RemoteConnectionState.Connecting;
            }
            previousStop?.Cancel();
            previousStop?.Dispose();
            previousSocket?.Dispose();
            socket.Options.AddSubProtocol(WebSocketProtocolConstants.SubProtocol);

            if (options.Auth.Enabled && !string.IsNullOrWhiteSpace(options.Auth.Scheme))
            {
                socket.Options.SetRequestHeader(
                    "Authorization",
                    string.IsNullOrEmpty(options.Auth.Token)
                        ? options.Auth.Scheme
                        : $"{options.Auth.Scheme} {options.Auth.Token}");
            }

            using var connectCts = CreateTimeoutTokenSource(options.ConnectTimeout);
            await socket.ConnectAsync(endpoint.ToUri(), connectCts.Token).ConfigureAwait(false);

            if (!string.Equals(
                    socket.SubProtocol,
                    WebSocketProtocolConstants.SubProtocol,
                    StringComparison.Ordinal))
            {
                WebSocketLog.Logger.Error("WebSocket server did not accept the required subprotocol.");
                throw new WebSocketProtocolException("WebSocket server did not accept the required subprotocol.");
            }

            await SendMessageAsync(
                    socket,
                    new HelloMessage
                    {
                        AuthScheme = options.Auth.Enabled ? options.Auth.Scheme : null,
                        AuthToken = options.Auth.Enabled ? options.Auth.Token : null,
                    },
                    options.HandshakeTimeout)
                .ConfigureAwait(false);

            using var handshakeCts = CreateTimeoutTokenSource(options.HandshakeTimeout);
            string helloAckJson;
            HelloAckMessage? helloAck;
            try
            {
                helloAckJson = await ReceiveTextMessageAsync(socket, handshakeCts.Token).ConfigureAwait(false);
                helloAck = WebSocketMessageSerializer.DeserializeMessage(helloAckJson) as HelloAckMessage;
            }
            catch (FormatException ex)
            {
                throw new WebSocketProtocolException("Invalid WebSocket handshake response.", ex);
            }

            if (helloAck == null)
            {
                WebSocketLog.Logger.Error($"Expected helloAck during WebSocket handshake: {helloAckJson}");
                throw new WebSocketProtocolException("Expected helloAck during WebSocket handshake.");
            }

            if (!helloAck.Accepted)
            {
                var reason = string.IsNullOrWhiteSpace(helloAck.Reason)
                    ? "WebSocket handshake rejected."
                    : helloAck.Reason;
                throw options.Auth.Enabled
                    ? new WebSocketAuthenticationException(reason)
                    : new WebSocketProtocolException(reason);
            }

            if (!IsCurrentGeneration(generation))
            {
                return;
            }

            lastReceivedAtUtc = DateTime.UtcNow;
            connectedAtUtc = lastReceivedAtUtc;
            lastError = null;
            effectiveHeartbeatInterval = ResolveHeartbeatInterval(
                options.HeartbeatInterval,
                helloAck.HeartbeatIntervalMilliseconds);
            effectiveHeartbeatTimeout = ResolveHeartbeatTimeout(
                options.HeartbeatTimeout,
                helloAck.HeartbeatTimeoutMilliseconds);
            if (!HeartbeatTiming.IsValid(effectiveHeartbeatInterval, effectiveHeartbeatTimeout))
            {
                throw new WebSocketProtocolException(
                    HeartbeatTiming.DescribeInvalidCombination(
                        effectiveHeartbeatInterval,
                        effectiveHeartbeatTimeout));
            }

            lock (connectionStateLock)
            {
                if (!IsCurrentGeneration(generation)) return;
                retryCount = 0;
                State = RemoteConnectionState.Connected;
            }
            receiveLoopTask = Task.Run(() => ReceiveLoopAsync(socket, generation));
            heartbeatTask = Task.Run(() => HeartbeatLoopAsync(socket, generation, effectiveHeartbeatInterval));
            subscribeAckTask = Task.Run(() => SubscribeAckLoopAsync(socket, generation));
            _ = RemoteEndpointBackgroundTask.Observe(receiveLoopTask);
            _ = RemoteEndpointBackgroundTask.Observe(heartbeatTask);
            _ = RemoteEndpointBackgroundTask.Observe(subscribeAckTask);
        }

        private async Task FlushSubscriptionsAsync()
        {
            await flushLock.WaitAsync(lifetimeCts.Token).ConfigureAwait(false);
            try { await FlushCurrentSubscriptionsAsync().ConfigureAwait(false); }
            finally { flushLock.Release(); }
        }

        private async Task FlushCurrentSubscriptionsAsync()
        {
            ClientWebSocket socket;
            long generation;
            CancellationToken connectionToken;
            lock (connectionStateLock)
            {
                if (disposed || State != RemoteConnectionState.Connected || webSocket == null) return;
                socket = webSocket;
                generation = Interlocked.Read(ref connectionGeneration);
                connectionToken = connectionCts!.Token;
            }

            foreach (var entry in subscriptionStore.Snapshot())
            {
                if (!IsCurrentGeneration(generation) || State != RemoteConnectionState.Connected)
                {
                    return;
                }

                if (!subscriptionStore.TryMarkSubscribeSending(entry.TokenId, out var sending))
                {
                    continue;
                }

                try
                {
                    await SendMessageAsync(
                            socket,
                            new SubscribeMessage
                            {
                                TokenId = sending!.TokenId,
                                Revision = sending.Revision,
                                Filter = sending.Filter,
                            },
                            options.SendTimeout, () => IsCurrentGeneration(generation) && subscriptionStore.IsCurrent(sending!))
                        .ConfigureAwait(false);
                    subscriptionStore.MarkSubscribeSent(sending!, DateTime.UtcNow);

                    if (options.ResubscribeInterval > TimeSpan.Zero)
                    {
                        await options.DelayProvider
                            .DelayAsync(options.ResubscribeInterval, connectionToken)
                            .ConfigureAwait(false);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    subscriptionStore.MarkSubscribeSendFailed(sending!, ex.Message);
                    HandleConnectionFailure(ex, generation);
                    return;
                }
            }
        }

        private async Task ReceiveLoopAsync(ClientWebSocket socket, long generation)
        {
            while (IsCurrentGeneration(generation) &&
                   !lifetimeCts.IsCancellationRequested &&
                   socket.State == WebSocketState.Open)
            {
                string json;
                try
                {
                    json = await ReceiveTextMessageAsync(socket, lifetimeCts.Token).ConfigureAwait(false);
                    if (IsCurrentGeneration(generation))
                    {
                        lastReceivedAtUtc = DateTime.UtcNow;
                    }
                }
                catch (OperationCanceledException) when (disposed || lifetimeCts.IsCancellationRequested)
                {
                    return;
                }
                catch (FormatException ex)
                {
                    if (IsCurrentGeneration(generation))
                    {
                        lastReceivedAtUtc = DateTime.UtcNow;
                        WebSocketLog.Logger.Warn($"WebSocket message discarded: endpoint={endpoint}, reason={ex.Message}");
                    }
                    continue;
                }
                catch (Exception ex)
                {
                    if (IsCurrentGeneration(generation) && State != RemoteConnectionState.Disposed)
                    {
                        HandleConnectionFailure(ex, generation);
                    }

                    return;
                }

                WebSocketMessageEnvelope message;
                try
                {
                    message = WebSocketMessageSerializer.DeserializeMessage(json);
                }
                catch (FormatException ex)
                {
                    if (IsCurrentGeneration(generation))
                    {
                        WebSocketLog.Logger.Warn($"WebSocket message discarded: endpoint={endpoint}, reason={ex.Message}");
                    }

                    continue;
                }

                try
                {
                    if (IsCurrentGeneration(generation))
                    {
                        messageHandler.HandleIncomingMessage(socket, generation, message);
                    }
                }
                catch (Exception ex)
                {
                    WebSocketLog.Logger.Warn($"WebSocket message handling failed: {ex.Message}");
                }
            }
        }

        private async Task HeartbeatLoopAsync(
            ClientWebSocket socket,
            long generation,
            TimeSpan heartbeatInterval)
        {
            if (heartbeatInterval <= TimeSpan.Zero)
            {
                return;
            }

            while (IsCurrentGeneration(generation) &&
                   !lifetimeCts.IsCancellationRequested &&
                   State == RemoteConnectionState.Connected)
            {
                try
                {
                    await options.DelayProvider
                        .DelayAsync(heartbeatInterval, lifetimeCts.Token)
                        .ConfigureAwait(false);
                    if (!IsCurrentGeneration(generation) || State != RemoteConnectionState.Connected)
                    {
                        return;
                    }

                    if (effectiveHeartbeatTimeout > TimeSpan.Zero &&
                        DateTime.UtcNow - lastReceivedAtUtc > effectiveHeartbeatTimeout)
                    {
                        socket.Abort();
                        HandleConnectionFailure(new TransientConnectionException("heartbeat timeout"), generation);
                        return;
                    }

                    await SendMessageAsync(socket, new PingMessage(), options.SendTimeout).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (disposed || lifetimeCts.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    if (IsCurrentGeneration(generation) && State != RemoteConnectionState.Disposed)
                    {
                        socket.Abort();
                        HandleConnectionFailure(ex, generation);
                    }

                    return;
                }
            }
        }

        internal static TimeSpan ResolveHeartbeatInterval(
            TimeSpan clientInterval,
            int serverIntervalMilliseconds)
        {
            return HeartbeatTiming.ResolvePositiveMinimum(clientInterval, serverIntervalMilliseconds);
        }

        internal static TimeSpan ResolveHeartbeatTimeout(
            TimeSpan clientTimeout,
            int? serverTimeoutMilliseconds)
        {
            return HeartbeatTiming.ResolvePositiveMinimum(clientTimeout, serverTimeoutMilliseconds);
        }

        private async Task SubscribeAckLoopAsync(ClientWebSocket socket, long generation)
        {
            if (options.SubscribeAckTimeout <= TimeSpan.Zero)
            {
                return;
            }

            var checkInterval = options.SubscribeAckTimeout < TimeSpan.FromSeconds(1)
                ? options.SubscribeAckTimeout
                : TimeSpan.FromSeconds(1);

            while (IsCurrentGeneration(generation) &&
                   !lifetimeCts.IsCancellationRequested &&
                   State == RemoteConnectionState.Connected)
            {
                try
                {
                    await Task.Delay(checkInterval, lifetimeCts.Token).ConfigureAwait(false);
                    if (!IsCurrentGeneration(generation) ||
                        State != RemoteConnectionState.Connected)
                    {
                        return;
                    }

                    var timedOutTokenIds = subscriptionStore.GetSubscribeAckTimeouts(
                        DateTime.UtcNow,
                        options.SubscribeAckTimeout);
                    foreach (var tokenId in timedOutTokenIds)
                    {
                        if (subscriptionStore.HasExhaustedSubscribeRetries(
                                tokenId,
                                options.MaxSubscribeRetryCount))
                        {
                            socket.Abort();
                            HandleConnectionFailure(
                                new TransientConnectionException(
                                    $"subscribe ACK timeout after {Math.Max(0, options.MaxSubscribeRetryCount)} retries"), generation);
                            return;
                        }

                        if (!subscriptionStore.TryPrepareRetryEntry(
                                tokenId,
                                options.MaxSubscribeRetryCount,
                                out RemoteSubscriptionEntry? sending))
                        {
                            continue;
                        }

                        await SendMessageAsync(
                                socket,
                                new SubscribeMessage
                                {
                                    TokenId = tokenId,
                                    Revision = sending!.Revision,
                                    Filter = sending.Filter,
                                },
                                options.SendTimeout, () => IsCurrentGeneration(generation) && subscriptionStore.IsCurrent(sending!))
                            .ConfigureAwait(false);
                        subscriptionStore.MarkSubscribeSent(sending!, DateTime.UtcNow);
                    }
                }
                catch (OperationCanceledException) when (disposed || lifetimeCts.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    if (IsCurrentGeneration(generation) && State != RemoteConnectionState.Disposed)
                    {
                        socket.Abort();
                        HandleConnectionFailure(ex, generation);
                    }

                    return;
                }
            }
        }

        private void ScheduleReconnectIfNeeded(string reason)
        {
            if (disposed ||
                State == RemoteConnectionState.Disposed ||
                State == RemoteConnectionState.Failed ||
                subscriptionStore.IsEffectivelyIdle)
            {
                return;
            }

            lock (reconnectLock)
            {
                if (disposed ||
                    State == RemoteConnectionState.Disposed ||
                    State == RemoteConnectionState.Failed ||
                    subscriptionStore.IsEffectivelyIdle)
                {
                    return;
                }

                if (reconnectTask != null && !reconnectTask.IsCompleted)
                {
                    return;
                }

                // Task.Run prevents the synchronous prefix (including external callbacks)
                // from executing on the thread holding reconnectLock.
                reconnectTask = Task.Run(() => ReconnectLoopAsync(reason));
                _ = RemoteEndpointBackgroundTask.Observe(reconnectTask);
            }
        }

        private async Task ReconnectLoopAsync(string reason)
        {
            while (!disposed &&
                   !lifetimeCts.IsCancellationRequested &&
                   State != RemoteConnectionState.Failed &&
                   !subscriptionStore.IsEffectivelyIdle)
            {
                lock (connectionStateLock)
                {
                    if (disposed || State is RemoteConnectionState.Failed or RemoteConnectionState.Connected) return;
                    retryCount++;
                    State = RemoteConnectionState.Reconnecting;
                }
                connectionStatusChanged(ConnectionStatusEvent.Reconnecting, reason, retryCount);

                try
                {
                    await options.DelayProvider
                        .DelayAsync(options.ReconnectPolicy.GetDelay(retryCount), lifetimeCts.Token)
                        .ConfigureAwait(false);

                    // double-check the conditions before attempting to reconnect, in case they have changed during the delay
                    if (disposed ||
                        lifetimeCts.IsCancellationRequested ||
                        State == RemoteConnectionState.Failed ||
                        subscriptionStore.IsEffectivelyIdle)
                    {
                        return;
                    }

                    await EnsureConnectedAsync().ConfigureAwait(false);

                    if (State == RemoteConnectionState.Connected)
                    {
                        return;
                    }

                    if (State == RemoteConnectionState.Failed)
                    {
                        return;
                    }
                }
                catch (OperationCanceledException) when (disposed || lifetimeCts.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    reason = ex.Message;
                    WebSocketLog.Logger.Warn($"WebSocket reconnect failed: {ex.Message}");
                }
            }
        }

        private async Task SendMessageAsync(ClientWebSocket socket, WebSocketMessageEnvelope message, TimeSpan timeout, Func<bool>? isValid = null)
        {
            using var sendCts = CreateTimeoutTokenSource(timeout);
            var bytes = WebSocketMessageSerializer.SerializeToUtf8Bytes(message);
            await sendLock.WaitAsync(sendCts.Token).ConfigureAwait(false);
            try
            {
                if (isValid != null && !isValid()) return;
                await socket
                    .SendAsync(bytes, WebSocketMessageType.Text, true, sendCts.Token)
                    .ConfigureAwait(false);
            }
            finally
            {
                sendLock.Release();
            }
        }

        private async Task<string> ReceiveTextMessageAsync(ClientWebSocket socket, CancellationToken cancellationToken)
        {
            return await WebSocketMessageReceiver.ReceiveTextAsync(socket, options.ReceiveBufferSize,
                options.MaxMessageBytes, cancellationToken).ConfigureAwait(false);
        }

        private bool IsCurrentGeneration(long generation)
        {
            return !disposed && Interlocked.Read(ref connectionGeneration) == generation;
        }

        private CancellationTokenSource CreateTimeoutTokenSource(TimeSpan timeout)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(lifetimeCts.Token);
            if (timeout > TimeSpan.Zero)
            {
                cts.CancelAfter(timeout);
            }

            return cts;
        }

        private void ValidateConfiguration()
        {
            if (options.Auth.Enabled && string.IsNullOrWhiteSpace(options.Auth.Scheme))
            {
                throw new WebSocketConfigurationException(
                    "WebSocket authentication scheme is required when authentication is enabled.");
            }

            if (!HeartbeatTiming.IsValid(options.HeartbeatInterval, options.HeartbeatTimeout))
            {
                throw new WebSocketConfigurationException(
                    HeartbeatTiming.DescribeInvalidCombination(
                        options.HeartbeatInterval,
                        options.HeartbeatTimeout));
            }
        }

        private void WaitForBackgroundTasksToStop()
        {
            var tasks = new[] { receiveLoopTask, heartbeatTask, subscribeAckTask, reconnectTask, refreshTask }
                .Where(task => task != null && !task.IsCompleted)
                .Cast<Task>()
                .Distinct()
                .ToArray();
            if (tasks.Length == 0)
            {
                return;
            }

            try
            {
                Task.WaitAll(tasks, TimeSpan.FromSeconds(1));
            }
            catch
            {
            }
        }

        private void HandleConnectionFailure(Exception exception)
            => HandleConnectionFailure(exception, Interlocked.Read(ref connectionGeneration));

        private void HandleConnectionFailure(Exception exception, long generation)
        {
            var terminal = exception is WebSocketConnectionException and not TransientConnectionException;
            var reason = exception.Message;
            ClientWebSocket? socket;
            CancellationTokenSource? connectionStop;
            lock (connectionStateLock)
            {
                if (!IsCurrentGeneration(generation) || State is RemoteConnectionState.Failed or RemoteConnectionState.Disposed) return;
                // Invalidate the old receive/send loops before a replacement can connect.
                Interlocked.Increment(ref connectionGeneration);
                socket = webSocket;
                webSocket = null;
                connectionStop = connectionCts;
                connectionCts = null;
                connectedAtUtc = null;
                lastError = reason;
                State = terminal ? RemoteConnectionState.Failed : RemoteConnectionState.Disconnected;
                if (terminal) subscriptionStore.MarkActiveOrPendingFailed(reason);
                else subscriptionStore.ResetForReconnect();
            }
            connectionStop?.Cancel();
            connectionStop?.Dispose();
            socket?.Dispose();
            if (terminal)
            {
                WebSocketLog.Logger.Error($"connect {endpoint} failed! \n" +
                    $"please check the port is right or remote app is new app using websocketcm or enable websocket server?");
            }
            try { connectionStatusChanged(terminal ? ConnectionStatusEvent.Failed : ConnectionStatusEvent.Disconnected, reason, retryCount); }
            finally { if (!terminal) ScheduleReconnectIfNeeded(reason); }
        }
    }
}
