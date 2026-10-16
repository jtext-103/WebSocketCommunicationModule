using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions;
using Jtext103.CFET2.WebSocketCommunicationModule.Exceptions;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using Newtonsoft.Json;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server
{
    public sealed partial class WebSocketSession : IDisposable
    {

        private readonly WebSocketCommunicationOptions options;
        private readonly WebSocketSubscriptionManager subscriptionManager;
        private readonly WebSocket socket;
        private readonly string? remoteAddress;
        private readonly ConcurrentDictionary<Guid, byte> remoteTokens = new();
        private readonly SemaphoreSlim sendLock = new(1, 1);
        private readonly CancellationTokenSource cts = new();
        private readonly object lifecycleLock = new();
        private readonly HashSet<Task<ServerSubscribeResult>> pendingAcks = new();
        private readonly DateTime createdAtUtc = DateTime.UtcNow;
        private DateTime? handshakeCompletedAtUtc;
        private DateTime lastReceivedAtUtc;
        private int disposed;

        internal WebSocketSession(

            WebSocketCommunicationOptions options,
            WebSocketSubscriptionManager subscriptionManager,
            WebSocket socket,
            string? remoteAddress = null)
        {

            this.options = options;
            this.subscriptionManager = subscriptionManager;
            this.socket = socket;
            this.remoteAddress = remoteAddress;
        }

        public Guid Id { get; } = Guid.NewGuid();

        public async Task RunAsync(CancellationToken serverToken)
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(serverToken, cts.Token);
            try
            {
                await HandshakeAsync(linkedCts.Token).ConfigureAwait(false);

                while (!linkedCts.IsCancellationRequested &&
                       socket.State == WebSocketState.Open)
                {
                    WebSocketMessageEnvelope message;
                    try
                    {
                        var json = await ReceiveTextMessageAsync(linkedCts.Token).ConfigureAwait(false);
                        message = WebSocketMessageSerializer.DeserializeMessage(json);
                    }
                    catch (FormatException ex)
                    {
                        lastReceivedAtUtc = DateTime.UtcNow;
                        WebSocketLog.Logger.Warn($"WebSocket message discarded: session={Id}, reason={ex.Message}");
                        continue;
                    }
                    switch (message)
                    {
                        case SubscribeMessage subscribe:
                            BeginSubscribe(subscribe, linkedCts.Token);
                            break;
                        case UnsubscribeMessage unsubscribe:
                            await UnsubscribeAsync(unsubscribe.TokenId, linkedCts.Token).ConfigureAwait(false);
                            break;
                        case PingMessage:
                            await SendAsync(new PongMessage(), linkedCts.Token).ConfigureAwait(false);
                            break;
                        case PongMessage:
                            break;
                        default:
                            await SendAsync(new ErrorMessage
                            {
                                Code = "unsupportedMessage",
                                Reason = $"Unsupported server message type: {message.MessageType}",
                            }, linkedCts.Token).ConfigureAwait(false);
                            break;
                    }
                }
            }
            catch (OperationCanceledException)
                when (linkedCts.IsCancellationRequested)
            {
            }
            catch (OperationCanceledException)
            {
                socket.Abort();
            }
            catch (WebSocketException)
            {
                // 客户端异常断开，正常结束 Session
                Dispose();
            }
            finally
            {
                Dispose();
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
            {
                return;
            }

            cts.Cancel();

            IDisposable cleanup;
            lock (lifecycleLock)
            {
                remoteTokens.Clear();
                cleanup = subscriptionManager.DetachSession(Id);
            }
            cleanup.Dispose();

            try
            {
                socket.Dispose();
            }
            catch
            {
            }

        }

        private async Task HandshakeAsync(CancellationToken cancellationToken)
        {
            using var handshakeCts = CreateTimeoutTokenSource(
                cancellationToken,
                options.ServerHandshakeTimeout);
            var json = await ReceiveTextMessageAsync(handshakeCts.Token).ConfigureAwait(false);
            var hello = WebSocketMessageSerializer.DeserializeMessage(json) as HelloMessage;
            WebSocketLog.Logger.Info($"accept hello: {hello == null} {hello?.ClientId}");
            if (hello == null)
            {
                await SendAsync(new ErrorMessage
                {
                    Code = "handshakeExpected",
                    Reason = "Expected hello message.",
                }, cancellationToken).ConfigureAwait(false);
                throw new FormatException("Expected hello message.");
            }

            await SendAsync(new HelloAckMessage
            {
                Accepted = true,
                HeartbeatIntervalMilliseconds = (int)options.HeartbeatInterval.TotalMilliseconds,
                HeartbeatTimeoutMilliseconds = (int)options.ServerIdleTimeout.TotalMilliseconds,
            }, cancellationToken).ConfigureAwait(false);
            handshakeCompletedAtUtc = DateTime.UtcNow;
        }

        private void BeginSubscribe(SubscribeMessage message, CancellationToken cancellationToken)
        {
            Task<ServerSubscribeResult> preparation;
            var validationFailure = false;
            IDisposable? cleanup = null;
            try
            {
                lock (lifecycleLock)
                {
                    if (Volatile.Read(ref disposed) != 0) return;
                    try
                    {
                        preparation = subscriptionManager.ReserveSubscription(this, message.TokenId, message.Revision,
                            message.Filter ?? new EventFilter(), out cleanup);
                    }
                    catch (Exception ex)
                    {
                        validationFailure = true;
                        preparation = Task.FromResult(new ServerSubscribeResult(message.Revision, false, ex.Message));
                    }
                    // Retransmissions wait for the same outcome without accumulating continuations.
                    if (!pendingAcks.Add(preparation)) return;
                }
            }
            finally { cleanup?.Dispose(); }
            _ = CompleteSubscribeAsync(message.TokenId, preparation, cancellationToken, validationFailure);
        }

        private async Task CompleteSubscribeAsync(Guid tokenId, Task<ServerSubscribeResult> preparation, CancellationToken cancellationToken, bool validationFailure)
        {
            try
            {
                var result = await preparation.ConfigureAwait(false);
                bool IsValid() => Volatile.Read(ref disposed) == 0 &&
                    (validationFailure || subscriptionManager.IsCurrent(Id, tokenId, preparation) ||
                     (!result.Accepted && result.Reason is "staleRevision" or "revisionConflict"));
                lock (lifecycleLock)
                {
                    if (!IsValid()) return;
                    if (result.Accepted) remoteTokens[tokenId] = 0;
                }
                var payload = SerializeMessage(new SubscribeAckMessage
                { TokenId = tokenId, Revision = result.Revision, Accepted = result.Accepted, Reason = result.Reason });
                await SendPayloadAsync(payload, cancellationToken, IsValid).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (preparation.IsCanceled || cancellationToken.IsCancellationRequested || cts.IsCancellationRequested) { }
            catch (Exception ex)
            {
                try { WebSocketLog.Logger.Error($"Subscribe acknowledgement failed: session={Id}, token={tokenId}: {ex}"); }
                catch { /* Session cleanup must survive a failing log provider. */ }
                Dispose();
            }
            finally { lock (lifecycleLock) pendingAcks.Remove(preparation); }
        }

        private async Task UnsubscribeAsync(Guid remoteTokenId, CancellationToken cancellationToken)
        {
            IDisposable cleanup;
            lock (lifecycleLock)
            {
                remoteTokens.TryRemove(remoteTokenId, out _);
                cleanup = subscriptionManager.DetachSubscription(Id, remoteTokenId);
            }
            cleanup.Dispose();

            await Task.CompletedTask.ConfigureAwait(false);
        }

        internal Task SendRemoteEventAsync(Guid remoteTokenId, EventArg eventArg)
        {
            return SendRemoteEventAsync(remoteTokenId, eventArg, 0);
        }

        internal async Task SendRemoteEventAsync(Guid remoteTokenId, EventArg eventArg, int performanceLevel,
            CancellationToken cancellationToken = default, Func<bool>? isValid = null)
        {
            if (cancellationToken.IsCancellationRequested || (isValid != null && !isValid())) return;
            if (Volatile.Read(ref disposed) != 0 || socket.State != WebSocketState.Open)
            {
                return;
            }

            byte[] payload;
            try
            {
                var message = new RemoteEventMessage
                {
                    TokenId = remoteTokenId,
                    PerformanceLevel = performanceLevel,
                    Source = performanceLevel == 1 ? eventArg.Source : null,
                    EventType = performanceLevel == 1 ? eventArg.EventType : null,
                    Value = performanceLevel == 1 ? eventArg.Sample?.ObjectVal : null,
                    Timestamp = performanceLevel == 1 ? eventArg.Sample?.Timestamp : null,
                    EventArg = performanceLevel == 0 ? CreateFullRemoteEvent(eventArg) : null,
                };
                payload = SerializeMessage(message);
            }
            catch (Exception ex) when (IsRecoverableSerializationException(ex))
            {
                WebSocketLog.Logger.Error(
                    $"Remote event serialization failed: session={Id}, token={remoteTokenId}, " +
                    $"performanceLevel={performanceLevel}, source={eventArg.Source}, " +
                    $"eventType={eventArg.EventType}, error={ex}");
                return;
            }

            try
            {
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
                await SendPayloadAsync(payload, linkedCts.Token, isValid).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested || cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                WebSocketLog.Logger.Error(
                    $"Remote event network send failed: session={Id}, token={remoteTokenId}, " +
                    $"performanceLevel={performanceLevel}, source={eventArg.Source}, " +
                    $"eventType={eventArg.EventType}, error={ex}");
                cts.Cancel();
                socket.Abort();
                throw;
            }
        }

        private static EventArg CreateFullRemoteEvent(EventArg eventArg)
        {
            return new EventArg
            {
                PerformanceLevel = 0,
                Host = eventArg.Host,
                Source = eventArg.Source,
                EventType = eventArg.EventType,
                Sample = eventArg.Sample,
            };
        }

        private async Task SendAsync(WebSocketMessageEnvelope message, CancellationToken cancellationToken)
        {
            var payload = SerializeMessage(message);
            await SendPayloadAsync(payload, cancellationToken).ConfigureAwait(false);
        }

        private byte[] SerializeMessage(WebSocketMessageEnvelope message)
        {
            var payload = WebSocketMessageSerializer.SerializeToUtf8Bytes(message);
            if (payload.Length > options.MaxMessageBytes)
            {
                throw new OutboundMessageTooLargeException(
                    $"WebSocket message exceeds the maximum allowed size: " +
                    $"{payload.Length} > {options.MaxMessageBytes} bytes.");
            }

            return payload;
        }

        private async Task SendPayloadAsync(byte[] payload, CancellationToken cancellationToken, Func<bool>? isValid = null)
        {
            using var sendCts = CreateTimeoutTokenSource(
                cancellationToken,
                options.ServerSendTimeout);

            await sendLock.WaitAsync(sendCts.Token).ConfigureAwait(false);
            try
            {
                if (isValid != null && !isValid()) return;
                await socket.SendAsync(
                    payload,
                    WebSocketMessageType.Text,
                    true,
                    sendCts.Token).ConfigureAwait(false);
            }
            finally
            {
                sendLock.Release();
            }
        }

        private static bool IsRecoverableSerializationException(Exception exception)
        {
            return exception is JsonException or
                NotSupportedException or
                OutboundMessageTooLargeException;
        }

        private async Task<string> ReceiveTextMessageAsync(CancellationToken cancellationToken)
        {
            using var idleCts = CreateTimeoutTokenSource(cancellationToken, options.ServerIdleTimeout);
            var json = await WebSocketMessageReceiver.ReceiveTextAsync(socket, options.ReceiveBufferSize,
                options.MaxMessageBytes, idleCts.Token).ConfigureAwait(false);
            lastReceivedAtUtc = DateTime.UtcNow;
            return json;
        }

        private static CancellationTokenSource CreateTimeoutTokenSource(
            CancellationToken cancellationToken,
            TimeSpan timeout)
        {
            var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (timeout > TimeSpan.Zero)
            {
                timeoutCts.CancelAfter(timeout);
            }

            return timeoutCts;
        }
    }
}
