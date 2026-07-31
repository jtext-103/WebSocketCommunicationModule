using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed class WebSocketEventReturnPhase5Tests : CFET2Host
{
    [TestInitialize]
    public void Init()
    {
        KillHubIfAlive();
        HubMaster.InjectHubToModule(this);
    }

    [TestCleanup]
    public void Cleanup()
    {
        KillHubIfAlive();
    }

    [TestMethod]
    public async Task RemoteEventMessage_ShouldRaiseRegisteredCallbackWithNormalizedHostAndRemoteSample()
    {
        using var server = EventPushServer.Start((tokenId, endpoint) => new RemoteEventMessage
        {
            EventArg = new EventArg("/thing/status", "Changed", new Status<int>(123)),
        });
        using var module = CreateModule();
        var receivedEvents = new List<EventArg>();
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source == "/thing/status")
            {
                receivedEvents.Add(arg);
            }
        });

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => receivedEvents.Count == 1));
        var received = receivedEvents.Single();
        Assert.AreEqual(EventHub.NormalizeHost(server.Endpoint), received.Host);
        Assert.AreEqual("/thing/status", received.Source);
        Assert.AreEqual("Changed", received.EventType);
        Assert.IsTrue(received.Sample.IsRemote);
        Assert.AreEqual(123, received.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task LightweightRemoteEvent_ShouldReachCoreWithLevel1RemoteSemantics()
    {
        using var server = EventPushServer.Start((tokenId, endpoint) => new RemoteEventMessage
        {
            TokenId = tokenId,
            PerformanceLevel = 1,
            Source = "/thing/status",
            EventType = "Changed",
            Value = 321,
        });
        using var module = CreateModule();
        EventArg? received = null;
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source == "/thing/status")
            {
                received = arg;
            }
        });

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint, performanceLevel: 1));

        Assert.IsTrue(await WaitUntilAsync(() => received != null));
        Assert.AreEqual(1, received!.PerformanceLevel);
        Assert.AreEqual(EventHub.NormalizeHost(server.Endpoint), received.Host);
        Assert.AreEqual("Changed", received.EventType);
        Assert.IsTrue(received.Sample.IsRemote);
        Assert.AreEqual(321L, received.Sample.GetVal<long>());
        Assert.IsFalse(received.Sample.Context.ContainsKey("serverOnlyContext"));
    }

    [TestMethod]
    public async Task RemoteEventMessage_WithTokenId_ShouldStillUseEventHubDispatch()
    {
        using var server = EventPushServer.Start((tokenId, endpoint) => new RemoteEventMessage
        {
            TokenId = tokenId,
            EventArg = new EventArg("/thing/status", "Changed", new Status<int>(456)),
        });
        using var module = CreateModule();
        MyHub.TryAddCommunicationModule(module);
        MyHub.StartCommunication();
        EventArg? received = null;

#pragma warning disable CS0612
        MyHub.EventHub.Subscribe(CreateFilter(server.Endpoint), arg => received = arg);
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => received != null));
        Assert.AreEqual(EventHub.NormalizeHost(server.Endpoint), received!.Host);
        Assert.AreEqual(456, received.Sample.GetVal<int>());
        Assert.IsTrue(received.Sample.IsRemote);
    }

    [TestMethod]
    public async Task RemoteEventMessage_WithUnknownTokenIdsField_ShouldStillUseEventHubDispatch()
    {
        using var server = EventPushServer.Start((tokenId, endpoint) => new RemoteEventMessageWithUnknownTokenIds
        {
            TokenIds = [tokenId],
            EventArg = new EventArg("/thing/status", "Changed", new Status<string>("ok")),
        });
        using var module = CreateModule();
        MyHub.TryAddCommunicationModule(module);
        MyHub.StartCommunication();
        EventArg? received = null;

#pragma warning disable CS0612
        MyHub.EventHub.Subscribe(CreateFilter(server.Endpoint), arg => received = arg);
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => received != null));
        Assert.AreEqual("ok", received!.Sample.GetVal<string>());
        Assert.AreEqual(EventHub.NormalizeHost(server.Endpoint), received.Host);
    }

    [TestMethod]
    public async Task RemoteEventMessage_WithStaleTokenId_ShouldBeDropped()
    {
        using var server = EventPushServer.StartMany((tokenId, endpoint) =>
        [
            new RemoteEventMessage
            {
                TokenId = Guid.NewGuid(),
                EventArg = new EventArg("/thing/stale", "Changed", new Status<int>(1)),
            },
            new RemoteEventMessage
            {
                EventArg = new EventArg("/thing/live", "Changed", new Status<int>(2)),
            },
        ]);
        using var module = CreateModule();
        var receivedEvents = new List<EventArg>();
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source.StartsWith("/thing/", StringComparison.Ordinal))
            {
                receivedEvents.Add(arg);
            }
        });

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint, "/thing/.*"));

        Assert.IsTrue(await WaitUntilAsync(() => receivedEvents.Any(arg => arg.Source == "/thing/live")));
        Assert.IsFalse(receivedEvents.Any(arg => arg.Source == "/thing/stale"));
    }

    [TestMethod]
    public async Task RemoteEventCallbackException_ShouldNotStopReceiveLoop()
    {
        using var server = EventPushServer.StartMany((tokenId, endpoint) =>
        [
            new RemoteEventMessage
            {
                EventArg = new EventArg("/thing/first", "Changed", new Status<int>(1)),
            },
            new RemoteEventMessage
            {
                EventArg = new EventArg("/thing/second", "Changed", new Status<int>(2)),
            },
        ]);
        using var module = CreateModule();
        var callbacks = 0;
        EventArg? second = null;
        module.RegisterEventCallback(arg =>
        {
            if (!arg.Source.StartsWith("/thing/", StringComparison.Ordinal))
            {
                return;
            }

            callbacks++;
            if (arg.Source == "/thing/first")
            {
                throw new InvalidOperationException("handler failed");
            }

            second = arg;
        });

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint, "/thing/.*"));

        Assert.IsTrue(await WaitUntilAsync(() => second != null));
        Assert.AreEqual(2, callbacks);
        Assert.AreEqual("/thing/second", second!.Source);
    }

    [TestMethod]
    public async Task ServerCloseAfterConnected_ShouldRaiseDisconnectedStatus()
    {
        using var server = EventPushServer.StartCloseAfterSubscribe();
        using var module = CreateModule();
        var statuses = new List<EventArg>();
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source == ConnectionStatusEvent.Source)
            {
                statuses.Add(arg);
            }
        });

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => statuses.Any(arg => arg.EventType == ConnectionStatusEvent.Connected)));
        Assert.IsTrue(await WaitUntilAsync(() => statuses.Any(arg => arg.EventType == ConnectionStatusEvent.Disconnected)));
        Assert.IsTrue(statuses.All(arg => arg.Host == EventHub.NormalizeHost(server.Endpoint)));
    }

    [TestMethod]
    public async Task UnsupportedMessageAfterHandshake_ShouldPermanentlyFailConnection()
    {
        using var server = EventPushServer.Start((_, _) => new UnsupportedMessage());
        using var module = CreateModule();
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() =>
            WebSocketClientTestHelper.GetOnlyEndpointClient(module).State == RemoteConnectionState.Failed));
        Assert.AreEqual(
            SubscriptionState.Failed,
            WebSocketClientTestHelper.GetSubscriptionForLocalToken(module, token.Id)?.State);
    }

    private WebSocketCommunicationModule CreateModule()
    {
        return new WebSocketCommunicationModule(new WebSocketCommunicationOptions
        {
            ConnectTimeout = TimeSpan.FromSeconds(3),
            HandshakeTimeout = TimeSpan.FromSeconds(3),
            SendTimeout = TimeSpan.FromSeconds(3),
            ReceiveBufferSize = 1024,
            ResubscribeInterval = TimeSpan.Zero,
        });
    }

    private Token CreateToken()
    {
        return new Token(MyHub.EventHub, "ws", "ws://127.0.0.1:1/cfet2/event/v2");
    }

    private static EventFilter CreateFilter(
        string endpoint,
        string source = "/thing/status",
        int performanceLevel = 0)
    {
        return new EventFilter(source, "Changed", performanceLevel, endpoint);
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> predicate, int timeoutMilliseconds = 3000)
    {
        var started = DateTime.UtcNow;
        while ((DateTime.UtcNow - started).TotalMilliseconds < timeoutMilliseconds)
        {
            if (predicate())
            {
                return true;
            }

            await Task.Delay(20);
        }

        return predicate();
    }

    private static void KillHubIfAlive()
    {
        try
        {
            Hub.KillMaster();
        }
        catch
        {
        }
    }

    private sealed class EventPushServer : IDisposable
    {
        private readonly CancellationTokenSource cts = new();
        private readonly Func<Guid, string, WebSocketMessageEnvelope[]> eventFactory;
        private readonly bool closeAfterSubscribe;
        private readonly HttpListener listener;
        private readonly List<WebSocket> sockets = new();
        private readonly object gate = new();
        private readonly Task acceptLoop;

        private EventPushServer(Func<Guid, string, WebSocketMessageEnvelope[]> eventFactory, bool closeAfterSubscribe)
        {
            this.eventFactory = eventFactory;
            this.closeAfterSubscribe = closeAfterSubscribe;
            Port = GetFreePort();
            Endpoint = $"ws://127.0.0.1:{Port}/cfet2/event/v2";
            listener = new HttpListener();
            listener.Prefixes.Add($"http://127.0.0.1:{Port}/cfet2/event/v2/");
            listener.Start();
            acceptLoop = Task.Run(AcceptLoopAsync);
        }

        public string Endpoint { get; }

        public int Port { get; }

        public static EventPushServer Start(Func<Guid, string, WebSocketMessageEnvelope> eventFactory)
        {
            return new EventPushServer((tokenId, endpoint) => [eventFactory(tokenId, endpoint)], closeAfterSubscribe: false);
        }

        public static EventPushServer StartMany(Func<Guid, string, WebSocketMessageEnvelope[]> eventFactory)
        {
            return new EventPushServer(eventFactory, closeAfterSubscribe: false);
        }

        public static EventPushServer StartCloseAfterSubscribe()
        {
            return new EventPushServer((_, _) => [], closeAfterSubscribe: true);
        }

        public void Dispose()
        {
            cts.Cancel();
            try
            {
                listener.Stop();
            }
            catch
            {
            }

            lock (gate)
            {
                foreach (var socket in sockets)
                {
                    socket.Dispose();
                }
            }

            try
            {
                acceptLoop.Wait(TimeSpan.FromSeconds(1));
            }
            catch
            {
            }

            listener.Close();
            cts.Dispose();
        }

        private async Task AcceptLoopAsync()
        {
            while (!cts.IsCancellationRequested)
            {
                HttpListenerContext context;
                try
                {
                    context = await listener.GetContextAsync();
                }
                catch
                {
                    return;
                }

                _ = Task.Run(() => HandleContextAsync(context));
            }
        }

        private async Task HandleContextAsync(HttpListenerContext context)
        {
            if (!context.Request.IsWebSocketRequest)
            {
                context.Response.StatusCode = 400;
                context.Response.Close();
                return;
            }

            var wsContext = await context.AcceptWebSocketAsync(WebSocketProtocolConstants.SubProtocol);
            lock (gate)
            {
                sockets.Add(wsContext.WebSocket);
            }

            try
            {
                await ReceiveTextAsync(wsContext.WebSocket, cts.Token);
                await SendAsync(wsContext.WebSocket, new HelloAckMessage { Accepted = true });

                var json = await ReceiveTextAsync(wsContext.WebSocket, cts.Token);
                var subscribe = (SubscribeMessage)WebSocketMessageSerializer.DeserializeMessage(json);
                await SendAsync(wsContext.WebSocket, new SubscribeAckMessage
                {
                    TokenId = subscribe.TokenId,
                    Accepted = true,
                });

                if (closeAfterSubscribe)
                {
                    await wsContext.WebSocket.CloseAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "test close",
                        CancellationToken.None);
                    return;
                }

                foreach (var message in eventFactory(subscribe.TokenId, Endpoint))
                {
                    await SendAsync(wsContext.WebSocket, message);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, cts.Token);
            }
            catch
            {
            }
        }

        private static async Task SendAsync(WebSocket socket, WebSocketMessageEnvelope message)
        {
            var bytes = WebSocketMessageSerializer.SerializeToUtf8Bytes(message);
            await socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
        }

        private static async Task<string> ReceiveTextAsync(WebSocket socket, CancellationToken cancellationToken)
        {
            var buffer = new byte[4096];
            using var stream = new MemoryStream();
            WebSocketReceiveResult result;
            do
            {
                result = await socket.ReceiveAsync(buffer, cancellationToken);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    throw new WebSocketException("Socket closed.");
                }

                stream.Write(buffer, 0, result.Count);
            }
            while (!result.EndOfMessage);

            return Encoding.UTF8.GetString(stream.ToArray());
        }

        private static int GetFreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
    }

    private sealed class RemoteEventMessageWithUnknownTokenIds : WebSocketMessageEnvelope
    {
        public RemoteEventMessageWithUnknownTokenIds()
            : base(WebSocketProtocolConstants.Event)
        {
        }

        public Guid[]? TokenIds { get; init; }

        public EventArg? EventArg { get; init; }
    }

    private sealed class UnsupportedMessage : WebSocketMessageEnvelope
    {
        public UnsupportedMessage()
            : base("unsupported")
        {
        }
    }
}
