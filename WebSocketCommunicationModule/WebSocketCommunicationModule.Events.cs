using System;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;

namespace Jtext103.CFET2.WebSocketCommunicationModule
{
    public partial class WebSocketCommunicationModule
    {
        // Core queues the same EventArg. Weak keys retain routing only as long as that event lives.
        private readonly System.Runtime.CompilerServices.ConditionalWeakTable<EventArg, EventRoute> eventRoutes = new();
        private sealed record EventRoute(RemoteEndpointKey Endpoint, bool IsResourceSubscription);

        public bool ShouldDispatch(Guid tokenId, EventArg eventArg) => !disposed &&
            (!eventRoutes.TryGetValue(eventArg, out var route) ||
             subscriptionAggregator.HasSubscriptionKind(tokenId, route.Endpoint, route.IsResourceSubscription));

        private void RaiseRemoteEvent(RemoteEndpointKey endpoint, EventArg eventArg)
        {
            if (eventArg == null || disposed)
            {
                return;
            }

            eventArg.Host = endpoint.NormalizedHost;
            if (eventArg.Sample != null)
            {
                eventArg.Sample.IsRemote = true;
            }

            InvokeEventCallback(eventArg);
        }



        private void InvokeEventCallback(EventArg eventArg)
        {
            Action<EventArg>? callback;
            lock (callbackLock)
            {
                callback = eventCallback;
            }

            if (callback == null || disposed)
            {
                return;
            }

            try
            {
                callback(eventArg);
            }
            catch
            {
                // Callback failures must not stop connection or recieve-loop state machines.
                WebSocketLog.Logger.Error("Attention: EventCallback failed! the reomte event won't recieved callback!");
            }
        }


    }
}
