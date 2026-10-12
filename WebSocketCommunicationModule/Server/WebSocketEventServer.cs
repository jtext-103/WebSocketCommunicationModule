using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Exceptions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;
using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server
{
    public sealed partial class WebSocketEventServer : IDisposable
    {

        private readonly WebSocketCommunicationOptions options;
        private readonly Hub hub;
        private readonly WebSocketSubscriptionManager subscriptionManager;
        private readonly ConcurrentDictionary<Guid, WebSocketSession> sessions = new();
        private readonly CancellationTokenSource cts = new();
        private int admittedSessionCount;
        private WebApplication? app;

        public WebSocketEventServer(WebSocketCommunicationOptions options, Hub hub)
        {

            this.options = options ?? throw new ArgumentNullException(nameof(options));
            this.hub = hub ?? throw new ArgumentNullException(nameof(hub));
            subscriptionManager = new WebSocketSubscriptionManager(
                hub,
                options.MaxFanOutConcurrency,
                options.MaxConcurrentFanOutEvents,
                options.MaxConcurrentSubscriptionPreparations,
                options.MaxPendingSubscriptionPreparations);
        }

        public bool IsStarted { get; private set; }

        public void Start()
        {
            if (IsStarted)
            {
                return;
            }

            ValidateConfiguration();
            if (options.ListenUri == null)
            {
                throw new InvalidOperationException("WebSocket server ListenUri is required.");
            }

            app = BuildKestrelApp(options.ListenUri);
            app.StartAsync().GetAwaiter().GetResult();
            IsStarted = true;
            
            Console.WriteLine("WebSocketServer running on "+options.ListenUri);
        }

        private void ValidateConfiguration()
        {
            if (!HeartbeatTiming.IsValid(options.HeartbeatInterval, options.ServerIdleTimeout))
            {
                throw new WebSocketConfigurationException(
                    HeartbeatTiming.DescribeInvalidCombination(
                        options.HeartbeatInterval,
                        options.ServerIdleTimeout));
            }
        }

        public void Dispose()
        {
            if (!IsStarted && app == null)
            {
                return;
            }

            IsStarted = false;
            cts.Cancel();

            try
            {
                using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
                app?.StopAsync(stopCts.Token).GetAwaiter().GetResult();
            }
            catch
            {
            }

            foreach (var session in sessions.Values)
            {
                session.Dispose();
            }

            sessions.Clear();
            subscriptionManager.Dispose();

            app?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            app = null;
            cts.Dispose();
        }

        private WebApplication BuildKestrelApp(Uri listenUri)
        {
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                Args = Array.Empty<string>(),
            });
            builder.WebHost.UseKestrel(kestrelOptions => ConfigureKestrelEndpoint(kestrelOptions, listenUri));

            var kestrelApp = builder.Build();
            kestrelApp.UseWebSockets();
            kestrelApp.Map(WebSocketProtocolConstants.EndpointPath, HandleContextAsync);
            return kestrelApp;
        }

        private static void ConfigureKestrelEndpoint(KestrelServerOptions kestrelOptions, Uri listenUri)
        {
            if (!string.Equals(listenUri.Scheme, "ws", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(listenUri.Scheme, "http", StringComparison.OrdinalIgnoreCase))
            {
                throw new NotSupportedException("WebSocket server currently supports ws/http only. https/wss is reserved for future support.");
            }

            kestrelOptions.Listen(ParseListenAddress(listenUri), listenUri.Port);
        }

        private static IPAddress ParseListenAddress(Uri listenUri)
        {
            if (IPAddress.TryParse(listenUri.Host, out var address))
            {
                return address;
            }

            if (string.Equals(listenUri.Host, "localhost", StringComparison.OrdinalIgnoreCase))
            {
                return IPAddress.Loopback;
            }

            if (string.Equals(listenUri.Host, "*", StringComparison.Ordinal) ||
                string.Equals(listenUri.Host, "+", StringComparison.Ordinal))
            {
                return IPAddress.Any;
            }

            var addresses = Dns.GetHostAddresses(listenUri.Host);
            return addresses.FirstOrDefault(address => address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                ?? addresses.FirstOrDefault()
                ?? throw new InvalidOperationException($"Unable to resolve WebSocket server ListenUri host '{listenUri.Host}'.");
        }

        private async Task HandleContextAsync(HttpContext context)
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return;
            }

            var requestedProtocols = context.WebSockets.WebSocketRequestedProtocols;
            if (!requestedProtocols.Contains(WebSocketProtocolConstants.SubProtocol, StringComparer.Ordinal))
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                Console.WriteLine($"requestedProtocols:{requestedProtocols}");
                return;
            }

            var admittedCount = Interlocked.Increment(ref admittedSessionCount);
            if (options.MaxServerSessions > 0 && admittedCount > options.MaxServerSessions)
            {
                Interlocked.Decrement(ref admittedSessionCount);
                context.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                return;
            }

            WebSocket webSocket;
            try
            {
                webSocket = await context.WebSockets.AcceptWebSocketAsync(WebSocketProtocolConstants.SubProtocol).ConfigureAwait(false);
            }
            catch
            {
                if (!context.Response.HasStarted)
                {
                    context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                }

                Interlocked.Decrement(ref admittedSessionCount);
                return;
            }

            var remoteAddress = context.Connection.RemoteIpAddress == null
                ? null
                : $"{context.Connection.RemoteIpAddress}:{context.Connection.RemotePort}";
            var session = new WebSocketSession( options, subscriptionManager, webSocket, remoteAddress);
            sessions[session.Id] = session;
            try
            {
                await session.RunAsync(cts.Token).ConfigureAwait(false);
            }
            finally
            {
                sessions.TryRemove(session.Id, out _);
                session.Dispose();
                Interlocked.Decrement(ref admittedSessionCount);
            }
        }
    }
}
