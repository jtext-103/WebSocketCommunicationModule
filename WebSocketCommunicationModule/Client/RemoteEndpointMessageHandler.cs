using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Jtext103.CFET2.Core.Log;
using System.Net.WebSockets;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed class RemoteEndpointMessageHandler
    {
        private readonly RemoteEndpointSubscriptionStore subscriptionStore;
        private readonly Func<long, bool> isCurrentGeneration;
        private readonly Func<ClientWebSocket, WebSocketMessageEnvelope, TimeSpan, Task> sendMessageAsync;
        private readonly TimeSpan sendTimeout;
        private readonly Action<DateTime> setLastReceivedAtUtc;
        private readonly Action<RemoteEventMessage> remoteEventReceived;
        private readonly Func<bool> isDisposed;
        private readonly Action<Exception, long> connectionFailure;


        public RemoteEndpointMessageHandler(
            RemoteEndpointSubscriptionStore subscriptionStore,
            Func<long, bool> isCurrentGeneration,
            Func<ClientWebSocket, WebSocketMessageEnvelope, TimeSpan, Task> sendMessageAsync,
            TimeSpan sendTimeout,
            Action<DateTime> setLastReceivedAtUtc,
            Action<RemoteEventMessage> remoteEventReceived,
            Func<bool> isDisposed,
            Action<Exception> connectionFailure,
            Action<Exception, long>? generationFailure = null)
        {
            this.subscriptionStore = subscriptionStore;
            this.isCurrentGeneration = isCurrentGeneration;
            this.sendMessageAsync = sendMessageAsync;
            this.sendTimeout = sendTimeout;
            this.setLastReceivedAtUtc = setLastReceivedAtUtc;
            this.remoteEventReceived = remoteEventReceived;
            this.isDisposed = isDisposed;
            this.connectionFailure = generationFailure ?? ((exception, _) => connectionFailure(exception));

        }

        public void HandleIncomingMessage(
            ClientWebSocket socket,
            long generation,
            WebSocketMessageEnvelope message)
        {
            switch (message)
            {
                case SubscribeAckMessage subscribeAck:
                    var failure = subscriptionStore.MarkSubscribeAck(subscribeAck);
                    if (failure != null)
                    {
                        WebSocketLog.Logger.Error(failure);
                    }
                    break;
                case RemoteEventMessage remoteEvent:
                    HandleRemoteEvent(remoteEvent);
                    break;
                case ErrorMessage error:
                    subscriptionStore.MarkError(error);
                    break;
                case PingMessage:
                    if (isCurrentGeneration(generation))
                    {
                        _ = RemoteEndpointBackgroundTask.Observe(
                            SendPongAsync(socket, generation));
                    }
                    break;
                case PongMessage:
                    if (isCurrentGeneration(generation))
                    {
                        setLastReceivedAtUtc(DateTime.UtcNow);
                    }
                    break;
            }
        }

        private async Task SendPongAsync(ClientWebSocket socket, long generation)
        {
            try
            {
                await sendMessageAsync(socket, new PongMessage(), sendTimeout).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (!isDisposed() && isCurrentGeneration(generation))
                {
                    connectionFailure(ex, generation);
                }
            }
        }

        private void HandleRemoteEvent(RemoteEventMessage message)
        {
            if (isDisposed() || message.EventArg == null)
            {
                return;
            }

            if (message.TokenId.HasValue && !subscriptionStore.Contains(message.TokenId.Value))
            {
                return;
            }

            remoteEventReceived(message);
        }
    }
}
