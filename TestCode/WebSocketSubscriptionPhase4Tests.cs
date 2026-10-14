using System.Collections.Concurrent;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using NLog;
using NLog.Config;
using NLog.Targets;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed class WebSocketSubscriptionPhase4Tests : CFET2Host
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
    public async Task SubscribeAgainToFailedGroup_ShouldLogEachCallWithoutSendingAgain()
    {
        var previousLogger = WebSocketLog.Logger;
        var target = new MemoryTarget { Layout = "${message}" };
        var config = new LoggingConfiguration();
        config.AddRule(LogLevel.Error, LogLevel.Fatal, target);
        using var factory = new LogFactory(config);
        try
        {
            using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
            {
                TokenId = tokenId, Accepted = false, Reason = "unsupported observation type"
            });
            using var module = CreateModule();
            WebSocketLog.Initialize(new Cfet2Logger(factory.GetLogger("WebSocket")));
            var first = CreateToken();
            module.Subscribe(first, CreateFilter(server.Endpoint));
            Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, first.Id)?.State == SubscriptionState.Failed));
            Assert.IsTrue(await WaitUntilAsync(() => target.Logs.Count == 1));
            module.Subscribe(CreateToken(), CreateFilter(server.Endpoint));
            module.Subscribe(first, CreateFilter(server.Endpoint));
            Assert.HasCount(3, target.Logs);
            StringAssert.Contains(target.Logs[1], "/thing/status");
            StringAssert.Contains(target.Logs[1], "unsupported observation type");
            StringAssert.Contains(target.Logs[2], "no retry sent");
            await Task.Delay(150);
            Assert.HasCount(1, server.SubscribeMessages);
            Assert.AreEqual(SubscriptionState.Failed, GetSubscription(module, first.Id)!.State);
        }
        finally
        {
            WebSocketLog.Initialize(previousLogger);
        }
    }

    [TestMethod]
    public async Task SubscribeAckAccepted_ShouldMoveSubscriptionToActive()
    {
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = true,
        });
        using var module = CreateModule();
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token.Id)?.State == SubscriptionState.Active));
        Assert.HasCount(1, server.SubscribeMessages);
        Assert.AreNotEqual(token.Id, server.SubscribeMessages.Single().TokenId);
        Assert.AreEqual("/thing/status", server.SubscribeMessages.Single().Filter?.SourcesAndTypes.Single().Source);
    }

    [TestMethod]
    public void Subscribe_UnsupportedPerformanceLevel_ShouldFailBeforeConnecting()
    {
        using var module = CreateModule();
        var token = CreateToken();
        var filter = CreateFilter("ws://127.0.0.1:1/cfet2/event/v2", performanceLevel: 2);

        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => module.Subscribe(token, filter));
        Assert.IsEmpty(GetConnections(module));
    }

    [TestMethod]
    public async Task SubscribeAckRejected_ShouldMoveSubscriptionToFailed()
    {
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = false,
            Reason = "bad filter",
        });
        using var module = CreateModule();
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token.Id)?.State == SubscriptionState.Failed));
        Assert.AreEqual("bad filter", GetSubscription(module, token.Id)!.LastError);
    }

    [TestMethod]
    public async Task ConcurrentSubscriptions_ShouldSendOneSubscribePerToken()
    {
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = true,
        });
        using var module = CreateModule();
        var token1 = CreateToken();
        var token2 = CreateToken();

        module.Subscribe(token1, CreateFilter(server.Endpoint, "/thing/a"));
        module.Subscribe(token2, CreateFilter(server.Endpoint, "/thing/b"));

        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 2));
        Assert.IsTrue(await WaitUntilAsync(() =>
            GetSubscription(module, token1.Id)?.State == SubscriptionState.Active &&
            GetSubscription(module, token2.Id)?.State == SubscriptionState.Active));
        CollectionAssert.AreEquivalent(
            GetMasterTokenIds(module, token1.Id).Concat(GetMasterTokenIds(module, token2.Id)).ToArray(),
            server.SubscribeMessages.Select(message => message.TokenId).ToArray());
        Assert.AreEqual(1, server.AcceptedCount);
    }

    [TestMethod]
    public async Task ConsecutiveSubscriptionsBeforeConnectionIsActive_ShouldActivateEverySubscription()
    {
        const int subscriptionCount = 10;
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = true,
        });
        using var module = CreateModule();
        var tokens = Enumerable.Range(1, subscriptionCount)
            .Select(_ => CreateToken())
            .ToArray();

        for (var index = 0; index < subscriptionCount; index++)
        {
            module.Subscribe(tokens[index], CreateFilter(server.Endpoint, "/thing/tick/" + (index + 1)));
        }

        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == subscriptionCount));
        Assert.IsTrue(await WaitUntilAsync(() => tokens.All(token =>
            GetSubscription(module, token.Id)?.State == SubscriptionState.Active)));
        Assert.AreEqual(1, server.AcceptedCount);
        CollectionAssert.AreEquivalent(
            Enumerable.Range(1, subscriptionCount).Select(index => "/thing/tick/" + index).ToArray(),
            server.SubscribeMessages
                .Select(message => message.Filter!.SourcesAndTypes.Single().Source)
                .ToArray());
    }

    [TestMethod]
    public async Task SubscribeAfterConnectionActive_ShouldFlushOnlyNewSubscription()
    {
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = true,
        });
        using var module = CreateModule();
        var token1 = CreateToken();
        var token2 = CreateToken();

        module.Subscribe(token1, CreateFilter(server.Endpoint, "/thing/a"));
        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token1.Id)?.State == SubscriptionState.Active));

        module.Subscribe(token2, CreateFilter(server.Endpoint, "/thing/b"));

        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token2.Id)?.State == SubscriptionState.Active));
        Assert.HasCount(2, server.SubscribeMessages);
        Assert.AreEqual(1, server.SubscribeMessages.Count(message => message.TokenId == GetMasterTokenIds(module, token1.Id).Single()));
        Assert.AreEqual(1, server.SubscribeMessages.Count(message => message.TokenId == GetMasterTokenIds(module, token2.Id).Single()));
    }

    [TestMethod]
    public async Task MixedPerformanceLevels_ShouldUpsertSameMasterTokenWithIncreasingRevision()
    {
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = true,
        });
        using var module = CreateModule();
        var level1Token = CreateToken();
        var level0Token = CreateToken();

        module.Subscribe(level1Token, CreateFilter(server.Endpoint, performanceLevel: 1));
        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, level1Token.Id)?.State == SubscriptionState.Active));
        module.Subscribe(level0Token, CreateFilter(server.Endpoint, performanceLevel: 0));
        Assert.IsTrue(await WaitUntilAsync(() =>
            server.SubscribeMessages.Count == 2 &&
            GetSubscription(module, level0Token.Id)?.State == SubscriptionState.Active));

        var masterTokenId = GetMasterTokenIds(module, level1Token.Id).Single();
        Assert.IsTrue(server.SubscribeMessages.All(message => message.TokenId == masterTokenId));
        Assert.AreEqual(0, server.SubscribeMessages[0].Revision);
        Assert.AreEqual(1, server.SubscribeMessages[1].Revision);
        Assert.AreEqual(1, server.SubscribeMessages[0].Filter!.PerformanceLevel);
        Assert.AreEqual(0, server.SubscribeMessages[1].Filter!.PerformanceLevel);

        module.UnSubScribe(level0Token);

        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 3));
        Assert.AreEqual(2, server.SubscribeMessages[2].Revision);
        Assert.AreEqual(1, server.SubscribeMessages[2].Filter!.PerformanceLevel);
    }

    [TestMethod]
    public async Task UnSubScribe_ShouldRemoveLocalStateFirstAndSendUnsubscribe()
    {
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = true,
        });
        using var module = CreateModule();
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));
        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token.Id)?.State == SubscriptionState.Active));
        var masterTokenId = GetMasterTokenIds(module, token.Id).Single();

        module.UnSubScribe(token);

        Assert.IsNull(GetSubscription(module, token.Id));
        Assert.IsTrue(await WaitUntilAsync(() => server.UnsubscribeMessages.Any(message => message.TokenId == masterTokenId)));
    }

    [TestMethod]
    public async Task UnSubScribeOneToken_ShouldKeepOtherTokenAndConnection()
    {
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = true,
        });
        using var module = CreateModule();
        var token1 = CreateToken();
        var token2 = CreateToken();

        module.Subscribe(token1, CreateFilter(server.Endpoint, "/thing/a"));
        module.Subscribe(token2, CreateFilter(server.Endpoint, "/thing/b"));
        Assert.IsTrue(await WaitUntilAsync(() =>
            GetSubscription(module, token1.Id)?.State == SubscriptionState.Active &&
            GetSubscription(module, token2.Id)?.State == SubscriptionState.Active));
        var token1MasterTokenId = GetMasterTokenIds(module, token1.Id).Single();

        module.UnSubScribe(token1);

        Assert.IsNull(GetSubscription(module, token1.Id));
        Assert.AreEqual(SubscriptionState.Active, GetSubscription(module, token2.Id)?.State);
        Assert.IsTrue(await WaitUntilAsync(() => server.UnsubscribeMessages.Any(message => message.TokenId == token1MasterTokenId)));
        Assert.HasCount(1, GetConnections(module));
        Assert.AreEqual(1, GetConnections(module).Single().SubscriptionCount);
    }

    [TestMethod]
    public async Task DuplicateUnSubScribe_ShouldBeIdempotent()
    {
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = true,
        });
        using var module = CreateModule();
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));
        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token.Id)?.State == SubscriptionState.Active));
        var masterTokenId = GetMasterTokenIds(module, token.Id).Single();

        module.UnSubScribe(token);
        module.UnSubScribe(token);

        Assert.IsTrue(await WaitUntilAsync(() => server.UnsubscribeMessages.Count(message => message.TokenId == masterTokenId) == 1));
        await Task.Delay(100);
        Assert.AreEqual(1, server.UnsubscribeMessages.Count(message => message.TokenId == masterTokenId));
    }

    [TestMethod]
    public async Task DisposeAndConcurrentSubscribeUnsubscribe_ShouldNotLeakUnexpectedExceptions()
    {
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = true,
        });
        var module = CreateModule();
        var errors = new ConcurrentQueue<Exception>();
        var tokens = Enumerable.Range(0, 100).Select(_ => CreateToken()).ToArray();
        var workers = tokens.Select((token, index) => Task.Run(() =>
        {
            try
            {
                module.Subscribe(token, CreateFilter(server.Endpoint, $"/thing/{index}"));
                module.UnSubScribe(token);
            }
            catch (ObjectDisposedException)
            {
            }
            catch (Exception ex)
            {
                errors.Enqueue(ex);
            }
        })).ToArray();

        await Task.WhenAll(workers.Append(Task.Run(module.Dispose)));

        Assert.IsEmpty(errors, string.Join(Environment.NewLine, errors.Select(error => error.ToString())));
        Assert.IsTrue(await WaitUntilAsync(() => GetConnections(module).Count == 0));
    }

    [TestMethod]
    public async Task ActiveConnectionAndConcurrentOperationsDuringDispose_ShouldNotLeakUnexpectedExceptions()
    {
        using var server = SubscriptionServer.Start(tokenId => new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = true,
        });
        var module = CreateModule();
        var activeToken = CreateToken();
        module.Subscribe(activeToken, CreateFilter(server.Endpoint, "/thing/active"));
        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, activeToken.Id)?.State == SubscriptionState.Active));

        var errors = new ConcurrentQueue<Exception>();
        var workers = Enumerable.Range(0, 50).Select(index => Task.Run(() =>
        {
            var token = CreateToken();
            try
            {
                module.Subscribe(token, CreateFilter(server.Endpoint, $"/thing/concurrent/{index}"));
                module.UnSubScribe(token);
            }
            catch (ObjectDisposedException)
            {
            }
            catch (Exception ex)
            {
                errors.Enqueue(ex);
            }
        })).ToArray();

        await Task.WhenAll(workers.Append(Task.Run(module.Dispose)));

        Assert.IsEmpty(errors, string.Join(Environment.NewLine, errors.Select(error => error.ToString())));
        Assert.IsTrue(await WaitUntilAsync(() => GetConnections(module).Count == 0));
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

    private static RemoteSubscriptionEntry? GetSubscription(WebSocketCommunicationModule module, Guid tokenId)
    {
        return WebSocketClientTestHelper.GetSubscriptionForLocalToken(module, tokenId);
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

    private sealed class SubscriptionServer : IDisposable
    {
        private readonly CancellationTokenSource cts = new();
        private readonly Func<Guid, SubscribeAckMessage> subscribeAckFactory;
        private readonly HttpListener listener;
        private readonly List<WebSocket> sockets = new();
        private readonly object gate = new();
        private readonly Task acceptLoop;

        private SubscriptionServer(Func<Guid, SubscribeAckMessage> subscribeAckFactory)
        {
            this.subscribeAckFactory = subscribeAckFactory;
            Port = GetFreePort();
            Endpoint = $"ws://127.0.0.1:{Port}/cfet2/event/v2";
            listener = new HttpListener();
            listener.Prefixes.Add($"http://127.0.0.1:{Port}/cfet2/event/v2/");
            listener.Start();
            acceptLoop = Task.Run(AcceptLoopAsync);
        }

        public string Endpoint { get; }

        public int Port { get; }

        public int AcceptedCount { get; private set; }

        private readonly System.Collections.Concurrent.ConcurrentQueue<SubscribeMessage> subscribeMessages = new();
        public IReadOnlyList<SubscribeMessage> SubscribeMessages => subscribeMessages.ToArray();

        private readonly System.Collections.Concurrent.ConcurrentQueue<UnsubscribeMessage> unsubscribeMessages = new();
        public IReadOnlyList<UnsubscribeMessage> UnsubscribeMessages => unsubscribeMessages.ToArray();

        public static SubscriptionServer Start(Func<Guid, SubscribeAckMessage> subscribeAckFactory)
        {
            return new SubscriptionServer(subscribeAckFactory);
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
                AcceptedCount++;
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
                                subscribeMessages.Enqueue(subscribe);
                            }

                            var ack = subscribeAckFactory(subscribe.TokenId);
                            await SendAsync(wsContext.WebSocket, new SubscribeAckMessage
                            {
                                TokenId = ack.TokenId,
                                Revision = subscribe.Revision,
                                Accepted = ack.Accepted,
                                Reason = ack.Reason,
                            });
                            break;
                        case UnsubscribeMessage unsubscribe:
                            lock (gate)
                            {
                                unsubscribeMessages.Enqueue(unsubscribe);
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
