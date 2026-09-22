using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed class WebSocketClientAggregationTests : CFET2Host
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
    public async Task SameRule_ShouldSendOneSubscribeForMultipleLocalTokens()
    {
        using var server = AggregationServer.Start();
        using var module = CreateModule();
        var token1 = CreateToken();
        var token2 = CreateToken();

        module.Subscribe(token1, CreateFilter(server.Endpoint, "/thing/status"));
        module.Subscribe(token2, CreateFilter(server.Endpoint, "/thing/status"));

        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 1));
        Assert.AreNotEqual(token1.Id, server.SubscribeMessages.Single().TokenId);
        Assert.AreNotEqual(token2.Id, server.SubscribeMessages.Single().TokenId);
        Assert.AreEqual(1, GetDictionaryCount(module, "subscriptions"));
        Assert.AreEqual(1, GetDictionaryCount(module, "clientSubscriptionGroups"));
        Assert.AreEqual(2, GetDictionaryCount(module, "localSubscriptionIndex"));
        Assert.AreEqual(1, GetConnections(module).Single().SubscriptionCount);
    }

    [TestMethod]
    public async Task Diagnostics_ShouldReportRemoteSubscriptionReadiness()
    {
        using var server = AggregationServer.Start();
        using var module = CreateModule();
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint, "/thing/status"));

        Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().RemoteActiveSubscriptionCount == 1));
        var status = module.GetStatus();
        var client = module.GetRemoteClient(server.Endpoint);

        Assert.AreEqual(1, status.RemoteClientCount);
        Assert.AreEqual(1, status.RemoteSubscriptionCount);
        Assert.AreEqual(0, status.RemotePendingSubscriptionCount);
        Assert.AreEqual(1, status.RemoteActiveSubscriptionCount);
        Assert.AreEqual(1, status.ClientAggregationGroupCount);
        Assert.AreEqual(1, status.LocalSubscriptionCount);
        Assert.IsNotNull(client);
        Assert.AreEqual("Connected", client.State);
        Assert.AreEqual("/thing/status", client.Subscriptions.Single().Source);
        Assert.IsTrue(module.PrintStatus().Contains("remoteActive=1", StringComparison.Ordinal));
        Assert.IsTrue(module.PrintRemoteClient(server.Endpoint).Contains("active=1", StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task MultiRuleFilter_ShouldSplitIntoOneRemoteSubscribePerRule()
    {
        using var server = AggregationServer.Start();
        using var module = CreateModule();
        var token = CreateToken();
        var filter = CreateFilter(server.Endpoint, "/thing/a");
        filter.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/b", EventType = "Changed" });

        module.Subscribe(token, filter);

        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 2));
        Assert.IsTrue(server.SubscribeMessages.All(message => message.Filter?.SourcesAndTypes.Count == 1));
        CollectionAssert.AreEquivalent(
            new[] { "/thing/a", "/thing/b" },
            server.SubscribeMessages.Select(message => message.Filter!.SourcesAndTypes.Single().Source).ToArray());
        Assert.AreEqual(2, GetDictionaryCount(module, "clientSubscriptionGroups"));
        Assert.HasCount(2, GetMasterTokenIds(module, token.Id));
    }

    [TestMethod]
    public async Task IntersectingFilters_ShouldBroadcastRemoteEventOnceAndInvokeEachLocalMatchOnce()
    {
        using var server = AggregationServer.Start();
        using var module = CreateModule();
        var token1 = CreateToken();
        var token2 = CreateToken();
        var filterA = CreateFilter(server.Endpoint, "/thing/a");
        var filterAB = CreateFilter(server.Endpoint, "/thing/a");
        filterAB.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/b", EventType = "Changed" });
        var token1Callbacks = 0;
        var token2Callbacks = 0;
        var broadcastsForA = 0;
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source != "/thing/a")
            {
                return;
            }

            broadcastsForA++;
            if (filterA.Predicate(arg))
            {
                token1Callbacks++;
            }

            if (filterAB.Predicate(arg))
            {
                token2Callbacks++;
            }
        });

        module.Subscribe(token1, filterA);
        module.Subscribe(token2, filterAB);

        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 2));
        var masterForA = server.SubscribeMessages
            .Single(message => message.Filter!.SourcesAndTypes.Single().Source == "/thing/a")
            .TokenId;

        await server.SendRemoteEventAsync(masterForA, "/thing/a");

        Assert.IsTrue(await WaitUntilAsync(() => broadcastsForA == 1));
        Assert.AreEqual(1, token1Callbacks);
        Assert.AreEqual(1, token2Callbacks);
        Assert.AreEqual(2, token1Callbacks + token2Callbacks);
    }

    [TestMethod]
    public async Task UnsubscribeOneLocalToken_ShouldKeepSharedRemoteSubscription()
    {
        using var server = AggregationServer.Start();
        using var module = CreateModule();
        var token1 = CreateToken();
        var token2 = CreateToken();

        module.Subscribe(token1, CreateFilter(server.Endpoint, "/thing/status"));
        module.Subscribe(token2, CreateFilter(server.Endpoint, "/thing/status"));
        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 1));

        module.UnSubScribe(token1);

        await Task.Delay(150);
        Assert.IsEmpty(server.UnsubscribeMessages);
        Assert.AreEqual(1, GetDictionaryCount(module, "clientSubscriptionGroups"));
        Assert.AreEqual(1, GetDictionaryCount(module, "subscriptions"));
        Assert.AreEqual(1, GetConnections(module).Single().SubscriptionCount);
    }

    [TestMethod]
    public async Task UnsubscribeLastLocalToken_ShouldSendOneRemoteUnsubscribeForMasterToken()
    {
        using var server = AggregationServer.Start();
        using var module = CreateModule();
        var token1 = CreateToken();
        var token2 = CreateToken();

        module.Subscribe(token1, CreateFilter(server.Endpoint, "/thing/status"));
        module.Subscribe(token2, CreateFilter(server.Endpoint, "/thing/status"));
        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 1));
        var masterTokenId = server.SubscribeMessages.Single().TokenId;

        module.UnSubScribe(token1);
        module.UnSubScribe(token2);

        Assert.IsTrue(await WaitUntilAsync(() => server.UnsubscribeMessages.Count == 1));
        Assert.AreEqual(masterTokenId, server.UnsubscribeMessages.Single().TokenId);
        Assert.AreEqual(0, GetDictionaryCount(module, "clientSubscriptionGroups"));
        Assert.AreEqual(0, GetDictionaryCount(module, "localSubscriptionIndex"));
        Assert.AreEqual(0, GetDictionaryCount(module, "subscriptions"));
    }

    [TestMethod]
    public async Task DuplicateRules_ShouldCreateOnlyOneGroup()
    {
        using var server = AggregationServer.Start();
        using var module = CreateModule();
        var token = CreateToken();
        var filter = CreateFilter(server.Endpoint, "/thing/a");
        filter.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/a", EventType = "Changed" });

        module.Subscribe(token, filter);

        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 1));
        Assert.AreEqual(1, GetDictionaryCount(module, "clientSubscriptionGroups"));
    }

    [TestMethod]
    public async Task SameLocalTokenRepeatedSubscribeWithSameRuleSet_ShouldBeIdempotent()
    {
        using var server = AggregationServer.Start();
        using var module = CreateModule();
        var token = CreateToken();
        var filterAB = CreateFilter(server.Endpoint, "/thing/a");
        filterAB.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/b", EventType = "Changed" });
        var filterBA = CreateFilter(server.Endpoint, "/thing/b");
        filterBA.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/a", EventType = "Changed" });

        module.Subscribe(token, filterAB);
        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 2));

        module.Subscribe(token, filterBA);

        await Task.Delay(150);
        Assert.HasCount(2, server.SubscribeMessages);
        Assert.IsEmpty(server.UnsubscribeMessages);
        Assert.AreEqual(2, GetDictionaryCount(module, "clientSubscriptionGroups"));
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

    private static EventFilter CreateFilter(string endpoint, string source)
    {
        return new EventFilter(source, "Changed", 0, endpoint);
    }

    private static int GetDictionaryCount(WebSocketCommunicationModule module, string fieldName)
    {
        return WebSocketClientTestHelper.GetDictionaryCount(module, fieldName);
    }

    private static Guid[] GetMasterTokenIds(WebSocketCommunicationModule module, Guid localTokenId)
    {
        return WebSocketClientTestHelper.GetMasterTokenIds(module, localTokenId);
    }

    private static List<RemoteEndpointClient> GetConnections(WebSocketCommunicationModule module)
    {
        return WebSocketClientTestHelper.GetEndpointClients(module);
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

    private sealed class AggregationServer : IDisposable
    {
        private readonly CancellationTokenSource cts = new();
        private readonly HttpListener listener;
        private readonly List<WebSocket> sockets = new();
        private readonly object gate = new();
        private readonly Task acceptLoop;

        private AggregationServer()
        {
            Port = GetFreePort();
            Endpoint = $"ws://127.0.0.1:{Port}/cfet2/event/v2";
            listener = new HttpListener();
            listener.Prefixes.Add($"http://127.0.0.1:{Port}/cfet2/event/v2/");
            listener.Start();
            acceptLoop = Task.Run(AcceptLoopAsync);
        }

        public string Endpoint { get; }

        public int Port { get; }

        public List<SubscribeMessage> SubscribeMessages { get; } = new();

        public List<UnsubscribeMessage> UnsubscribeMessages { get; } = new();

        public static AggregationServer Start()
        {
            return new AggregationServer();
        }

        public async Task SendRemoteEventAsync(Guid tokenId, string source)
        {
            WebSocket socket;
            lock (gate)
            {
                socket = sockets.Single();
            }

            await SendAsync(socket, new RemoteEventMessage
            {
                TokenId = tokenId,
                EventArg = new EventArg(source, "Changed", new Status<int>(1)),
            });
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

                while (!cts.IsCancellationRequested && wsContext.WebSocket.State == WebSocketState.Open)
                {
                    var json = await ReceiveTextAsync(wsContext.WebSocket, cts.Token);
                    var message = WebSocketMessageSerializer.DeserializeMessage(json);
                    switch (message)
                    {
                        case SubscribeMessage subscribe:
                            lock (gate)
                            {
                                SubscribeMessages.Add(subscribe);
                            }

                            await SendAsync(wsContext.WebSocket, new SubscribeAckMessage
                            {
                                TokenId = subscribe.TokenId,
                                Accepted = true,
                            });
                            break;
                        case UnsubscribeMessage unsubscribe:
                            lock (gate)
                            {
                                UnsubscribeMessages.Add(unsubscribe);
                            }

                            break;
                    }
                }
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
}
