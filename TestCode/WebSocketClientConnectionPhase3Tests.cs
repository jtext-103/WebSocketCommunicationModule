using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed class WebSocketClientConnectionPhase3Tests : CFET2Host
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
    public async Task Subscribe_ShouldConnectSendHelloAndRaiseConnected()
    {
        using var server = TestWebSocketServer.Start(hello => new HelloAckMessage
        {
            Accepted = true,
            HeartbeatIntervalMilliseconds = 30000,
        });
        using var module = CreateModule();
        EventArg? status = null;
        module.RegisterEventCallback(arg => status = arg);

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount == 1));
        Assert.IsTrue(await WaitUntilAsync(() => status?.EventType == ConnectionStatusEvent.Connected));
        Assert.AreEqual(WebSocketProtocolConstants.SubProtocol, server.AcceptedSubProtocols.Single());
        Assert.AreEqual(WebSocketProtocolConstants.Hello, server.Hellos.Single().MessageType);
        Assert.AreEqual(RemoteConnectionState.Connected, GetOnlyConnection(module).State);
    }

    [TestMethod]
    public async Task ConcurrentSubscriptionsToSameEndpoint_ShouldReuseOneConnection()
    {
        using var server = TestWebSocketServer.Start(_ => new HelloAckMessage { Accepted = true });
        using var module = CreateModule();

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint, "/thing/a"));
        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint, "/thing/b"));

        Assert.IsTrue(await WaitUntilAsync(() => GetOnlyConnection(module).State == RemoteConnectionState.Connected));
        await Task.Delay(200);
        Assert.AreEqual(1, server.AcceptedCount, $"Expected one accepted connection, got {server.AcceptedCount}.");
        Assert.AreEqual(2, GetOnlyConnection(module).SubscriptionCount);
    }

    [TestMethod]
    public async Task Subscribe_WithHostOnlyEndpoint_ShouldConnectToV2Path()
    {
        using var server = TestWebSocketServer.Start(_ => new HelloAckMessage { Accepted = true });
        using var module = CreateModule();

        module.Subscribe(CreateToken(), CreateFilter(server.HostOnlyEndpoint));

        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount == 1));
        var connection = GetOnlyConnection(module);
        Assert.IsTrue(await WaitUntilAsync(() => connection.State == RemoteConnectionState.Connected));
        Assert.AreEqual(RemoteConnectionState.Connected, connection.State);
        Assert.AreEqual(server.HostOnlyEndpoint, connection.Endpoint.NormalizedHost);
        Assert.AreEqual(server.Endpoint, connection.Endpoint.ToUri().ToString().TrimEnd('/'));
    }



    [TestMethod]
    public async Task SubscriptionsToDifferentEndpoints_ShouldCreateDifferentConnections()
    {
        using var server1 = TestWebSocketServer.Start(_ => new HelloAckMessage { Accepted = true });
        using var server2 = TestWebSocketServer.Start(_ => new HelloAckMessage { Accepted = true });
        using var module = CreateModule();

        module.Subscribe(CreateToken(), CreateFilter(server1.Endpoint, "/thing/a"));
        module.Subscribe(CreateToken(), CreateFilter(server2.Endpoint, "/thing/b"));

        Assert.IsTrue(await WaitUntilAsync(() => server1.AcceptedCount == 1 && server2.AcceptedCount == 1));
        Assert.IsTrue(await WaitUntilAsync(() =>
            GetConnections(module).Count == 2 &&
            GetConnections(module).All(connection => connection.State == RemoteConnectionState.Connected)));
        Assert.HasCount(2, GetConnections(module));
        CollectionAssert.AreEquivalent(
            new[] { RemoteConnectionState.Connected, RemoteConnectionState.Connected },
            GetConnections(module).Select(connection => connection.State).ToArray());
    }

    [TestMethod]
    public async Task RejectedHelloAck_ShouldMarkConnectionFailedAndRaiseFailed()
    {
        using var server = TestWebSocketServer.Start(_ => new HelloAckMessage
        {
            Accepted = false,
            Reason = "nope",
        });
        using var module = CreateModule();
        EventArg? status = null;
        module.RegisterEventCallback(arg => status = arg);

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => status?.EventType == ConnectionStatusEvent.Failed));
        Assert.AreEqual(RemoteConnectionState.Failed, GetOnlyConnection(module).State);
        Assert.AreEqual("nope", ((ConnectionStatusPayload)status!.Sample.ObjectVal).Reason);
    }

    [TestMethod]
    public async Task InvalidNegotiatedHeartbeatTiming_ShouldMarkConnectionFailed()
    {
        using var server = TestWebSocketServer.Start(_ => new HelloAckMessage
        {
            Accepted = true,
            HeartbeatIntervalMilliseconds = 1000,
            HeartbeatTimeoutMilliseconds = 2000,
        });
        using var module = CreateModule();
        EventArg? status = null;
        module.RegisterEventCallback(arg => status = arg);

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => status?.EventType == ConnectionStatusEvent.Failed));
        Assert.AreEqual(RemoteConnectionState.Failed, GetOnlyConnection(module).State);
        StringAssert.Contains(
            ((ConnectionStatusPayload)status!.Sample.ObjectVal).Reason,
            "at least 3 times");
    }

    [TestMethod]
    public async Task InvalidLocalHeartbeatTiming_ShouldMarkConnectionFailedAsConfigurationError()
    {
        using var server = TestWebSocketServer.Start(_ => new HelloAckMessage { Accepted = true });
        using var module = CreateModule(
            heartbeatInterval: TimeSpan.FromSeconds(10),
            heartbeatTimeout: TimeSpan.FromSeconds(20));
        EventArg? status = null;
        module.RegisterEventCallback(arg => status = arg);

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => status?.EventType == ConnectionStatusEvent.Failed));
        Assert.AreEqual(0, server.AcceptedCount);
        StringAssert.Contains(
            ((ConnectionStatusPayload)status!.Sample.ObjectVal).Reason,
            "at least 3 times");
    }

    [TestMethod]
    public async Task MissingSubProtocol_ShouldMarkConnectionFailed()
    {
        using var server = TestWebSocketServer.Start(
            _ => new HelloAckMessage { Accepted = true },
            acceptSubProtocol: false);
        using var module = CreateModule();
        EventArg? status = null;
        module.RegisterEventCallback(arg => status = arg);

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => status?.EventType == ConnectionStatusEvent.Failed));
        Assert.AreEqual(RemoteConnectionState.Failed, GetOnlyConnection(module).State);
    }

    [TestMethod]
    public async Task BadHandshakeMessage_ShouldMarkConnectionFailed()
    {
        using var server = TestWebSocketServer.StartRawAck("""
            {
              "messageType": "helloAck",
              "protocol": "old.protocol",
              "version": 2,
              "accepted": true
            }
            """);
        using var module = CreateModule();
        EventArg? status = null;
        module.RegisterEventCallback(arg => status = arg);

        module.Subscribe(CreateToken(), CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => status?.EventType == ConnectionStatusEvent.Failed));
        Assert.AreEqual(RemoteConnectionState.Failed, GetOnlyConnection(module).State);
    }

    [TestMethod]
    public async Task WrongHandshakeMessageType_ShouldMarkConnectionAndSubscriptionFailed()
    {
        using var server = TestWebSocketServer.StartRawAck("""
            {
              "messageType": "subscribeAck",
              "protocol": "cfet2.websocket.event",
              "version": 2,
              "tokenId": "00000000-0000-0000-0000-000000000000",
              "accepted": true
            }
            """);
        using var module = CreateModule();
        var token = CreateToken();
        EventArg? status = null;
        module.RegisterEventCallback(arg => status = arg);

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => status?.EventType == ConnectionStatusEvent.Failed));
        Assert.AreEqual(RemoteConnectionState.Failed, GetOnlyConnection(module).State);
        var subscription = GetSubscription(module, token.Id);
        Assert.IsNotNull(subscription);
        Assert.AreEqual(SubscriptionState.Failed, subscription.State);
        StringAssert.Contains(subscription.LastError, "Expected helloAck");
    }

    [TestMethod]
    public async Task MissingHandshakeAck_ShouldRemainPendingAndReconnect()
    {
        using var server = TestWebSocketServer.StartWithoutAck();
        using var module = CreateModule(
            handshakeTimeout: TimeSpan.FromMilliseconds(100),
            reconnectPolicy: new ReconnectPolicy
            {
                InitialDelay = TimeSpan.FromMilliseconds(10),
                FastRetryCount = 5,
                MaxDelay = TimeSpan.FromMilliseconds(10),
            });
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount >= 2));
        Assert.AreNotEqual(RemoteConnectionState.Failed, GetOnlyConnection(module).State);
        var subscription = GetSubscription(module, token.Id);
        Assert.IsNotNull(subscription);
        Assert.AreEqual(SubscriptionState.Pending, subscription.State);
    }

    [TestMethod]
    public void RemoteEndpointKey_ShouldNormalizeHostAndAppendV2Path()
    {
        var endpoint = RemoteEndpointKey.Parse("WS://Example.COM:9000/cfet2/event/v2/");

        Assert.AreEqual("ws", endpoint.Scheme);
        Assert.AreEqual("example.com", endpoint.Host);
        Assert.AreEqual(9000, endpoint.Port);
        Assert.AreEqual("/cfet2/event/v2", endpoint.Path);
        Assert.AreEqual("ws://example.com:9000", endpoint.NormalizedHost);
        Assert.AreEqual("ws://example.com:9000/cfet2/event/v2", endpoint.ToUri().ToString().TrimEnd('/'));

        var hostOnlyEndpoint = RemoteEndpointKey.Parse("WS://Example.COM:9000");
        Assert.AreEqual("ws://example.com:9000", hostOnlyEndpoint.NormalizedHost);
        Assert.AreEqual("ws://example.com:9000/cfet2/event/v2", hostOnlyEndpoint.ToUri().ToString().TrimEnd('/'));

        var secureEndpoint = RemoteEndpointKey.Parse("wss://Example.COM:9443");
        Assert.AreEqual("wss", secureEndpoint.Scheme);

        AssertThrowsFormatException(() => RemoteEndpointKey.Parse("ws://example.com:9000/legacy"));
        AssertThrowsFormatException(() => RemoteEndpointKey.Parse("http://example.com:9000/cfet2/event/v2"));
        AssertThrowsFormatException(() => RemoteEndpointKey.Parse("ftp://example.com:9000/cfet2/event/v2"));
    }

    private WebSocketCommunicationModule CreateModule(
        TimeSpan? handshakeTimeout = null,
        ReconnectPolicy? reconnectPolicy = null,
        TimeSpan? heartbeatInterval = null,
        TimeSpan? heartbeatTimeout = null)
    {
        return new WebSocketCommunicationModule(new WebSocketCommunicationOptions
        {
            ConnectTimeout = TimeSpan.FromSeconds(3),
            HandshakeTimeout = handshakeTimeout ?? TimeSpan.FromSeconds(3),
            SendTimeout = TimeSpan.FromSeconds(3),
            ReceiveBufferSize = 1024,
            HeartbeatInterval = heartbeatInterval ?? TimeSpan.FromSeconds(30),
            HeartbeatTimeout = heartbeatTimeout ?? TimeSpan.FromSeconds(90),
            ReconnectPolicy = reconnectPolicy ?? new ReconnectPolicy(),
        });
    }

    private Token CreateToken()
    {
        return new Token(MyHub.EventHub, "ws", "ws://127.0.0.1:1/cfet2/event/v2");
    }

    private static EventFilter CreateFilter(string endpoint, string source = "/thing/status")
    {
        return new EventFilter(source, "Changed", 0, endpoint);
    }

    private static RemoteEndpointClient GetOnlyConnection(WebSocketCommunicationModule module)
    {
        return WebSocketClientTestHelper.GetOnlyEndpointClient(module);
    }

    private static List<RemoteEndpointClient> GetConnections(WebSocketCommunicationModule module)
    {
        return WebSocketClientTestHelper.GetEndpointClients(module);
    }

    private static RemoteSubscriptionEntry? GetSubscription(WebSocketCommunicationModule module, Guid tokenId)
    {
        return WebSocketClientTestHelper.GetSubscriptionForLocalToken(module, tokenId);
    }

    private static Guid[] GetMasterTokenIds(WebSocketCommunicationModule module, Guid localTokenId)
    {
        return WebSocketClientTestHelper.GetMasterTokenIds(module, localTokenId);
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

    private static void AssertThrowsFormatException(Action action)
    {
        try
        {
            action();
        }
        catch (FormatException)
        {
            return;
        }

        Assert.Fail("Expected FormatException.");
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

    private sealed class TestWebSocketServer : IDisposable
    {
        private readonly CancellationTokenSource cts = new();
        private readonly Func<HelloMessage, string?> ackFactory;
        private readonly bool acceptSubProtocol;
        private readonly HttpListener listener;
        private readonly List<WebSocket> sockets = new();
        private readonly object gate = new();
        private readonly Task acceptLoop;

        private TestWebSocketServer(Func<HelloMessage, string?> ackFactory, bool acceptSubProtocol)
        {
            this.ackFactory = ackFactory;
            this.acceptSubProtocol = acceptSubProtocol;
            Port = GetFreePort();
            Endpoint = $"ws://127.0.0.1:{Port}/cfet2/event/v2";
            listener = new HttpListener();
            listener.Prefixes.Add($"http://127.0.0.1:{Port}/cfet2/event/v2/");
            listener.Start();
            acceptLoop = Task.Run(AcceptLoopAsync);
        }

        public int Port { get; }

        public string Endpoint { get; }

        public string HostOnlyEndpoint => $"ws://127.0.0.1:{Port}";

        public int AcceptedCount { get; private set; }

        public List<HelloMessage> Hellos { get; } = new();

        public List<string?> AcceptedSubProtocols { get; } = new();

        public static TestWebSocketServer Start(
            Func<HelloMessage, HelloAckMessage> ackFactory,
            bool acceptSubProtocol = true)
        {
            return new TestWebSocketServer(
                hello => WebSocketMessageSerializer.Serialize(ackFactory(hello)),
                acceptSubProtocol);
        }

        public static TestWebSocketServer StartRawAck(string rawAck)
        {
            return new TestWebSocketServer(_ => rawAck, acceptSubProtocol: true);
        }

        public static TestWebSocketServer StartWithoutAck()
        {
            return new TestWebSocketServer(_ => null, acceptSubProtocol: true);
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

            WebSocketContext wsContext;
            try
            {
                wsContext = await context.AcceptWebSocketAsync(
                    acceptSubProtocol ? WebSocketProtocolConstants.SubProtocol : null);
            }
            catch
            {
                context.Response.StatusCode = 500;
                context.Response.Close();
                return;
            }

            lock (gate)
            {
                AcceptedCount++;
                AcceptedSubProtocols.Add(wsContext.WebSocket.SubProtocol);
                sockets.Add(wsContext.WebSocket);
            }

            try
            {
                var helloJson = await ReceiveTextAsync(wsContext.WebSocket, cts.Token);
                var hello = (HelloMessage)WebSocketMessageSerializer.DeserializeMessage(helloJson);
                lock (gate)
                {
                    Hellos.Add(hello);
                }

                var rawAck = ackFactory(hello);
                if (rawAck == null)
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, cts.Token);
                    return;
                }

                var ack = Encoding.UTF8.GetBytes(rawAck);
                await wsContext.WebSocket.SendAsync(
                    ack,
                    WebSocketMessageType.Text,
                    true,
                    cts.Token);

                await Task.Delay(Timeout.InfiniteTimeSpan, cts.Token);
            }
            catch
            {
            }
        }

        private static async Task<string> ReceiveTextAsync(WebSocket socket, CancellationToken cancellationToken)
        {
            var buffer = new byte[4096];
            using var stream = new MemoryStream();
            WebSocketReceiveResult result;
            do
            {
                result = await socket.ReceiveAsync(buffer, cancellationToken);
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
}
