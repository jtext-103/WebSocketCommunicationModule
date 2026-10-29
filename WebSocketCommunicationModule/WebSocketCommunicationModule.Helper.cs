using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using System;
using System.Collections.Generic;
using System.Text;

namespace Jtext103.CFET2.WebSocketCommunicationModule
{
    public partial class WebSocketCommunicationModule
    {

        private void EnqueueClientPlan(ClientSubscriptionPlan plan, DeferredWebSocketLogs logs)
        {
            foreach (var reused in plan.ReusedSubscriptions)
            {
                if (endpointClients.TryGetValue(reused.Endpoint, out var existing))
                {
                    var failure = existing.GetSubscriptionFailure(reused.MasterTokenId, reused.Revision);
                    if (failure != null)
                    {
                        logs.Error(failure + " (existing failed subscription reused; no retry sent)");
                    }
                }
            }

            if (plan.IsEmpty)
            {
                return;
            }

            foreach (var action in plan.Actions)
            {
                switch (action.Kind)
                {
                    case RemoteEndpointActionKind.Subscribe:
                        var client = endpointClients.GetOrAdd(action.Endpoint, CreateRemoteEndpointClient);
                        client.EnqueueSubscribe(action.MasterTokenId, action.Revision, action.RemoteFilter!, logs);
                        break;
                    case RemoteEndpointActionKind.Unsubscribe:
                        if (endpointClients.TryGetValue(action.Endpoint, out var existingClient))
                        {
                            existingClient.EnqueueUnsubscribe(action.MasterTokenId, logs);
                        }
                        break;
                }
            }
        }
        private RemoteEndpointClient CreateRemoteEndpointClient(RemoteEndpointKey endpoint)
        {
            var client = new RemoteEndpointClient(endpoint, options);
            client.RemoteEventReceived += OnRemoteEventReceived;
            client.ConnectionStatusChanged += OnConnectionStatusChanged;
            client.Idle += OnClientIdle;
            return client;
        }

        private void OnRemoteEventReceived(RemoteEndpointClient client, RemoteEventMessage message)
        {
            if (message.EventArg == null)
            {
                return;
            }

            if (message.TokenId is Guid masterTokenId && client.GetSubscription(masterTokenId) is { } subscription)
                eventRoutes.Add(message.EventArg, new EventRoute(client.Endpoint, subscription.Filter.IsResourceSubscription));
            RaiseRemoteEvent(client.Endpoint, message.EventArg);
        }

        private void OnConnectionStatusChanged(RemoteEndpointClient client, ConnectionStatusEventArgs args)
        {
            RaiseConnectionStatus(client.Endpoint, args.EventType, args.Reason, args.RetryCount);
        }

        private void RaiseConnectionStatus(RemoteEndpointKey endpoint, string eventType, string reason, int retryCount)
        {
            if (disposed)
            {
                return;
            }

            WebSocketLog.Logger.Info($"Connent Status Changed {endpoint}: {eventType}");

            var eventArg = ConnectionStatusEvent.Create(
                endpoint.NormalizedHost,
                endpoint.Scheme,
                eventType,
                reason,
                retryCount);

            InvokeEventCallback(eventArg);
        }

        private void OnClientIdle(RemoteEndpointClient idleClient)
        {
            RemoteEndpointClient? removedClient = null;
            lock (lifecycleLock)
            {
                if (!disposed &&
                    endpointClients.TryGetValue(idleClient.Endpoint, out var current) &&
                    ReferenceEquals(current, idleClient) &&
                    idleClient.IsIdle &&
                    !subscriptionAggregator.HasLocalSubscriptions(idleClient.Endpoint))
                {
                    endpointClients.TryRemove(idleClient.Endpoint, out removedClient);
                }
            }

            if (removedClient != null)
            {
                removedClient.RemoteEventReceived -= OnRemoteEventReceived;
                removedClient.ConnectionStatusChanged -= OnConnectionStatusChanged;
                removedClient.Idle -= OnClientIdle;
                removedClient.Dispose();
            }
        }
    }
}
