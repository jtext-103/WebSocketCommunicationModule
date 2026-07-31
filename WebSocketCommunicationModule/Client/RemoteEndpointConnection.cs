using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Exceptions;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using System.Net.WebSockets;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed partial class RemoteEndpointConnection : IDisposable
    {
        private readonly RemoteEndpointKey endpoint;
        private readonly WebSocketCommunicationOptions options;
        private readonly ICfet2Logger logger;
        private readonly RemoteEndpointSubscriptionStore subscriptionStore;
        private readonly Action<string, string, int> connectionStatusChanged;
        private readonly CancellationTokenSource lifetimeCts = new();
        private readonly SemaphoreSlim sendLock = new(1, 1);
        private readonly SemaphoreSlim connectLock = new(1, 1);
        private readonly object reconnectLock = new();
        private readonly RemoteEndpointMessageHandler messageHandler;
        private ClientWebSocket? webSocket;
        private Task? receiveLoopTask;
        private Task? reconnectTask;
        private Task? heartbeatTask;
        private Task? subscribeAckTask;
        private DateTime lastReceivedAtUtc;
        private TimeSpan effectiveHeartbeatInterval;
        private TimeSpan effectiveHeartbeatTimeout;
        private long connectionGeneration;
        private int retryCount;
        private volatile bool disposed;

        public RemoteEndpointConnection(
            RemoteEndpointKey endpoint,
            WebSocketCommunicationOptions options,
            ICfet2Logger logger,
            RemoteEndpointSubscriptionStore subscriptionStore,
            Action<RemoteEventMessage> remoteEventReceived,
            Action<string, string, int> connectionStatusChanged)
        {
            this.endpoint = endpoint;
            this.options = options;
            this.logger = logger;
            this.subscriptionStore = subscriptionStore;
            this.connectionStatusChanged = connectionStatusChanged;
            effectiveHeartbeatInterval = options.HeartbeatInterval;
            effectiveHeartbeatTimeout = options.HeartbeatTimeout;
            messageHandler = new RemoteEndpointMessageHandler(
                subscriptionStore,
                IsCurrentGeneration,
                SendMessageAsync,
                options.SendTimeout,
                value => lastReceivedAtUtc = value,
                remoteEventReceived,
                () => disposed,
                HandleConnectionFailure);
        }

        public RemoteConnectionState State { get; private set; }

        internal TimeSpan EffectiveHeartbeatInterval => effectiveHeartbeatInterval;

        internal TimeSpan EffectiveHeartbeatTimeout => effectiveHeartbeatTimeout;

        public async Task EnsureConnectedAsync()
        {
            // double-check the conditions before attempting to connect, in case they have changed while waiting for the lock
            if (disposed || State == RemoteConnectionState.Disposed || subscriptionStore.IsEffectivelyIdle)
            {
                return;
            }

            await connectLock.WaitAsync(lifetimeCts.Token).ConfigureAwait(false);
            try
            {
                if (disposed || State == RemoteConnectionState.Disposed || subscriptionStore.IsEffectivelyIdle)
                {
                    return;
                }

                if (webSocket?.State == WebSocketState.Open && State == RemoteConnectionState.Connected)
                {
                    await FlushSubscriptionsAsync().ConfigureAwait(false);
                    return;
                }

                State = RemoteConnectionState.Connecting;
                await ConnectAndHandshakeAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (disposed || lifetimeCts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                HandleConnectionFailure(ex);
            }
            finally
            {
                connectLock.Release();
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
                        logger.Warn(
                            $"WebSocket unsubscribe {masterTokenId} failed after {retryCount} retries: {ex.Message}");
                        socket.Abort();
                        HandleConnectionFailure(
                            new TransientConnectionException(
                                $"unsubscribe failed after {retryCount} retries",
                                ex));
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
            if (disposed)
            {
                return;
            }

            disposed = true;
            State = RemoteConnectionState.Disposed;
            lifetimeCts.Cancel();
            webSocket?.Dispose();
            WaitForBackgroundTasksToStop();
        }

        private async Task ConnectAndHandshakeAsync()
        {
            ValidateConfiguration();
            webSocket?.Dispose();
            var socket = new ClientWebSocket();
            var generation = Interlocked.Increment(ref connectionGeneration);
            webSocket = socket;
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
                logger.Error("WebSocket server did not accept the required subprotocol.");
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
                logger.Error($"Expected helloAck during WebSocket handshake: {helloAckJson}");
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

            retryCount = 0;
            State = RemoteConnectionState.Connected;
            receiveLoopTask = RemoteEndpointBackgroundTask.Observe(ReceiveLoopAsync(socket, generation));
            heartbeatTask = RemoteEndpointBackgroundTask.Observe(
                HeartbeatLoopAsync(socket, generation, effectiveHeartbeatInterval));
            subscribeAckTask = RemoteEndpointBackgroundTask.Observe(
                SubscribeAckLoopAsync(socket, generation));
            connectionStatusChanged(ConnectionStatusEvent.Connected, "handshake completed", 0);
            await FlushSubscriptionsAsync().ConfigureAwait(false);
        }

        private async Task FlushSubscriptionsAsync()
        {
            if (disposed || State != RemoteConnectionState.Connected)
            {
                return;
            }

            foreach (var entry in subscriptionStore.Snapshot())
            {
                if (disposed || State != RemoteConnectionState.Connected)
                {
                    return;
                }

                if (!subscriptionStore.TryMarkSubscribeSending(entry.TokenId))
                {
                    continue;
                }

                try
                {
                    await SendMessageAsync(
                            new SubscribeMessage
                            {
                                TokenId = entry.TokenId,
                                Revision = entry.Revision,
                                Filter = entry.Filter,
                            },
                            options.SendTimeout)
                        .ConfigureAwait(false);
                    subscriptionStore.MarkSubscribeSent(entry.TokenId, DateTime.UtcNow);

                    if (options.ResubscribeInterval > TimeSpan.Zero)
                    {
                        await options.DelayProvider
                            .DelayAsync(options.ResubscribeInterval, lifetimeCts.Token)
                            .ConfigureAwait(false);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    subscriptionStore.MarkSubscribeSendFailed(entry.TokenId, ex.Message);
                    HandleConnectionFailure(ex);
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
                catch (Exception ex)
                {
                    if (IsCurrentGeneration(generation) && State != RemoteConnectionState.Disposed)
                    {
                        HandleConnectionFailure(ex is FormatException
                            ? new WebSocketProtocolException("Invalid WebSocket message received.", ex)
                            : ex);
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
                        HandleConnectionFailure(
                            new WebSocketProtocolException("Invalid WebSocket message received.", ex));
                    }

                    return;
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
                    logger.Warn($"WebSocket message handling failed: {ex.Message}");
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
                        HandleConnectionFailure(new TransientConnectionException("heartbeat timeout"));
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
                        HandleConnectionFailure(ex);
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
                                    $"subscribe ACK timeout after {Math.Max(0, options.MaxSubscribeRetryCount)} retries"));
                            return;
                        }

                        if (!subscriptionStore.TryPrepareSubscribeRetry(
                                tokenId,
                                options.MaxSubscribeRetryCount,
                                out var filter,
                                out var revision))
                        {
                            continue;
                        }

                        await SendMessageAsync(
                                socket,
                                new SubscribeMessage
                                {
                                    TokenId = tokenId,
                                    Revision = revision,
                                    Filter = filter,
                                },
                                options.SendTimeout)
                            .ConfigureAwait(false);
                        subscriptionStore.MarkSubscribeSent(tokenId, DateTime.UtcNow);
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
                        HandleConnectionFailure(ex);
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

                reconnectTask = RemoteEndpointBackgroundTask.Observe(ReconnectLoopAsync(reason));
            }
        }

        private async Task ReconnectLoopAsync(string reason)
        {
            while (!disposed &&
                   !lifetimeCts.IsCancellationRequested &&
                   State != RemoteConnectionState.Failed &&
                   !subscriptionStore.IsEffectivelyIdle)
            {
                retryCount++;
                State = RemoteConnectionState.Reconnecting;
                subscriptionStore.ResetForReconnect();
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
                    logger.Warn($"WebSocket reconnect failed: {ex.Message}");
                }
            }
        }

        private async Task SendMessageAsync(WebSocketMessageEnvelope message, TimeSpan timeout)
        {
            var socket = webSocket;
            if (socket == null)
            {
                throw new InvalidOperationException("WebSocket has not been created.");
            }

            await SendMessageAsync(socket, message, timeout).ConfigureAwait(false);
        }

        private async Task SendMessageAsync(ClientWebSocket socket, WebSocketMessageEnvelope message, TimeSpan timeout)
        {
            using var sendCts = CreateTimeoutTokenSource(timeout);
            var bytes = WebSocketMessageSerializer.SerializeToUtf8Bytes(message);
            await sendLock.WaitAsync(sendCts.Token).ConfigureAwait(false);
            try
            {
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
            var buffer = new byte[options.ReceiveBufferSize];
            using var stream = new MemoryStream();
            WebSocketReceiveResult result;
            do
            {
                result = await socket
                    .ReceiveAsync(buffer, cancellationToken)
                    .ConfigureAwait(false);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    throw new WebSocketException("WebSocket closed while waiting for a message.");
                }

                if (result.MessageType != WebSocketMessageType.Text)
                {
                    throw new FormatException("Only text WebSocket messages are supported.");
                }

                stream.Write(buffer, 0, result.Count);
                if (stream.Length > options.MaxMessageBytes)
                {
                    throw new FormatException("WebSocket message exceeds the maximum allowed size.");
                }
            }
            while (!result.EndOfMessage);

            return System.Text.Encoding.UTF8.GetString(stream.ToArray());
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
            var tasks = new[] { receiveLoopTask, heartbeatTask, subscribeAckTask, reconnectTask }
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
        {
            if (exception is WebSocketConnectionException and not TransientConnectionException)
            {
                MarkConnectionFailed(exception.Message);
                return;
            }

            MarkConnectionDisconnected(exception.Message);
        }

        private void MarkConnectionDisconnected(string reason)
        {
            if (State == RemoteConnectionState.Failed ||
                State == RemoteConnectionState.Disposed)
            {
                return;
            }

            webSocket?.Dispose();
            webSocket = null;
            State = RemoteConnectionState.Disconnected;
            subscriptionStore.ResetForReconnect();
            connectionStatusChanged(ConnectionStatusEvent.Disconnected, reason, retryCount);
            ScheduleReconnectIfNeeded(reason);
        }

        private void MarkConnectionFailed(string reason)
        {
            logger.Error($"connect {endpoint} failed! \n" +
                $"please check the port is right or remote app is new app using websocketcm or enable websocket server?");
            webSocket?.Dispose();
            webSocket = null;
            if (State != RemoteConnectionState.Disposed)
            {
                State = RemoteConnectionState.Failed;
            }

            subscriptionStore.MarkActiveOrPendingFailed(reason);
            connectionStatusChanged(ConnectionStatusEvent.Failed, reason, retryCount);
        }
    }
}
