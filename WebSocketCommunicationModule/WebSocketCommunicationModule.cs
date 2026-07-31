using Jtext103.CFET2.Core.Communication;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Jtext103.CFET2.WebSocketCommunicationModule.Server;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Jtext103.CFET2.WebSocketCommunicationModule
{
    public partial class WebSocketCommunicationModule : IEventCommunicationModule
    {
        public override string[] ProtocolNames => ["ws", "http"];

        private readonly WebSocketCommunicationOptions options;
        private readonly ConcurrentDictionary<RemoteEndpointKey, RemoteEndpointClient> endpointClients = new();
        private readonly ClientSubscriptionAggregator subscriptionAggregator = new();
        private readonly object lifecycleLock = new();
        private readonly object callbackLock = new();
        private Action<EventArg>? eventCallback;
        private WebSocketEventServer? server;
        private volatile bool started;
        private volatile bool disposed;

        private readonly ICfet2Logger logger = Cfet2LogManager.GetLogger("WebSocket");

        public WebSocketCommunicationModule(Uri uri)
            : this(new WebSocketCommunicationOptions { ListenUri = uri, EnableServer = uri != null })
        {
        }

        public WebSocketCommunicationModule(WebSocketCommunicationOptions options)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public override void Start()
        {
            lock (lifecycleLock)
            {
                ThrowIfDisposed();
                if (started)
                {
                    return;
                }

                base.Start();
                if (options.EnableServer)
                {
                    server ??= new WebSocketEventServer(logger, options, MyHub.EventHub);
                    server.Start();
                }

                logger.Info($"WebSocket running on {options.ListenUri}");

                started = true;
            }
        }

        public override void Dispose()
        {
            RemoteEndpointClient[] clients;
            lock (lifecycleLock)
            {
                if (disposed)
                {
                    return;
                }

                disposed = true;
                clients = endpointClients.Values.ToArray();
                endpointClients.Clear();
                subscriptionAggregator.Clear();
            }

            server?.Dispose();
            server = null;

            foreach (var client in clients)
            {
                client.RemoteEventReceived -= OnRemoteEventReceived;
                client.ConnectionStatusChanged -= OnConnectionStatusChanged;
                client.Idle -= OnClientIdle;
                client.Dispose();
            }

            lock (callbackLock)
            {
                eventCallback = null;
            }
        }

        public override void RegisterEventCallback(Action<EventArg> hander)
        {
            logger.Info($"WebSocket RegisterEventCallback");
            if (hander == null)
            {
                throw new ArgumentNullException(nameof(hander));
            }

            ThrowIfDisposed();
            lock (callbackLock)
            {
                eventCallback = hander;
            }
        }

        public override void Subscribe(Token token, EventFilter filter)
        {
            if (token == null)
            {
                throw new ArgumentNullException(nameof(token));
            }

            if (filter == null)
            {
                throw new ArgumentNullException(nameof(filter));
            }

            lock (lifecycleLock)
            {
                ThrowIfDisposed();
                if (filter.PerformanceLevel is not 0 and not 1)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(filter),
                        $"Unsupported PerformanceLevel: {filter.PerformanceLevel}");
                }

                var plan = subscriptionAggregator.Subscribe(token, filter);
                EnqueueClientPlan(plan);
            }

            logger.Info($"Subscribe: {token.Host} filter Count: {filter.SourcesAndTypes.Count}");
        }

        public override void UnSubScribe(Token token)
        {
            if (token == null)
            {
                return;
            }

            lock (lifecycleLock)
            {
                if (disposed)
                {
                    return;
                }

                var plan = subscriptionAggregator.Unsubscribe(token);
                EnqueueClientPlan(plan);
            }
        }


        private void ThrowIfDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(nameof(WebSocketCommunicationModule));
            }
        }
    }
}
