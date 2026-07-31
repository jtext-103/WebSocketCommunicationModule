using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
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
        private readonly Action<Exception> connectionFailure;

        public RemoteEndpointMessageHandler(
            RemoteEndpointSubscriptionStore subscriptionStore,
            Func<long, bool> isCurrentGeneration,
            Func<ClientWebSocket, WebSocketMessageEnvelope, TimeSpan, Task> sendMessageAsync,
            TimeSpan sendTimeout,
            Action<DateTime> setLastReceivedAtUtc,
            Action<RemoteEventMessage> remoteEventReceived,
            Func<bool> isDisposed,
            Action<Exception> connectionFailure)
        {
            this.subscriptionStore = subscriptionStore;
            this.isCurrentGeneration = isCurrentGeneration;
            this.sendMessageAsync = sendMessageAsync;
            this.sendTimeout = sendTimeout;
            this.setLastReceivedAtUtc = setLastReceivedAtUtc;
            this.remoteEventReceived = remoteEventReceived;
            this.isDisposed = isDisposed;
            this.connectionFailure = connectionFailure;
        }

        public void HandleIncomingMessage(
            ClientWebSocket socket,
            long generation,
            WebSocketMessageEnvelope message)
        {
            switch (message)
            {
                case SubscribeAckMessage subscribeAck:
                    subscriptionStore.MarkSubscribeAck(subscribeAck);
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
                    connectionFailure(ex);
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
