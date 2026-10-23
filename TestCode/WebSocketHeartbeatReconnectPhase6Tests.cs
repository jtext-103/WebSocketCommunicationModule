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
public sealed class WebSocketHeartbeatReconnectPhase6Tests : CFET2Host
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
    public async Task ConnectionFailure_CancelsOldPacingWaitAndResubscribesOnNewSocket()
    {
        using var server = Phase6Server.Start(respondToPing: true);
        var delay = new FirstPacingDelay();
        using var client = new RemoteEndpointClient(RemoteEndpointKey.Parse(server.Endpoint), new WebSocketCommunicationOptions
        {
            HeartbeatInterval = TimeSpan.Zero, HeartbeatTimeout = TimeSpan.Zero,
            ResubscribeInterval = FirstPacingDelay.Interval, DelayProvider = delay,
            ReconnectPolicy = FastReconnectPolicy(),
        });
        var token = Guid.NewGuid();
        client.EnqueueSubscribe(token, CreateFilter(server.Endpoint));
        await delay.Entered.Task.WaitAsync(TimeSpan.FromSeconds(3));
        await server.FirstSubscriptionReceived.WaitAsync(TimeSpan.FromSeconds(3));
        await server.SimulateSendFailureAsync("/thing/status", "Changed", 1);
        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount >= 2 && server.SubscribeMessages.Count >= 2));
        Assert.IsTrue(delay.Cancelled);
        Assert.AreEqual(token, server.SubscribeMessages.Last().TokenId);
        Assert.AreEqual(RemoteConnectionState.Connected, client.State);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task SubscribeWaitingForSendGate_DoesNotSendRemovedOrReplacedAttempt(bool remove)
    {
        using var server = Phase6Server.Start(respondToPing: true);
        using var client = new RemoteEndpointClient(RemoteEndpointKey.Parse(server.Endpoint), new WebSocketCommunicationOptions
        {
            HeartbeatInterval = TimeSpan.Zero, HeartbeatTimeout = TimeSpan.Zero, ResubscribeInterval = TimeSpan.Zero,
        });
        var initial = Guid.NewGuid();
        client.EnqueueSubscribe(initial, CreateFilter(server.Endpoint));
        Assert.IsTrue(await WaitUntilAsync(() => client.GetSubscription(initial)?.State == SubscriptionState.Active));
        var connection = (RemoteEndpointConnection)typeof(RemoteEndpointClient)
            .GetField("connection", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)!;
        var sendGate = (SemaphoreSlim)typeof(RemoteEndpointConnection)
            .GetField("sendLock", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(connection)!;
        await sendGate.WaitAsync();
        var token = Guid.NewGuid();
        try
        {
            client.EnqueueSubscribe(token, 0, CreateFilter(server.Endpoint, "/thing/replaced"));
            Assert.IsTrue(await WaitUntilAsync(() => client.GetSubscription(token)?.SubscribeSendInProgress == true));
            if (remove) client.EnqueueUnsubscribe(token);
            else client.EnqueueSubscribe(token, 1, CreateFilter(server.Endpoint, "/thing/replaced", 1));
            Assert.IsTrue(await WaitUntilAsync(() => remove ? client.GetSubscription(token) == null : client.GetSubscription(token)?.Revision == 1));
        }
        finally { sendGate.Release(); }
        await connection.EnsureConnectedAsync().WaitAsync(TimeSpan.FromSeconds(3));
        Assert.IsFalse(server.SubscribeMessages.Any(m => m.TokenId == token && m.Revision == 0));
        if (!remove) Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Any(m => m.TokenId == token && m.Revision == 1)));
    }

    [TestMethod]
    public async Task SubscribeSendTimeout_RefreshWorkerRecoversAndProcessesLaterUpdates()
    {
        using var server = Phase6Server.Start(respondToPing: true);
        using var client = new RemoteEndpointClient(RemoteEndpointKey.Parse(server.Endpoint), new WebSocketCommunicationOptions
        {
            HeartbeatInterval = TimeSpan.Zero, HeartbeatTimeout = TimeSpan.Zero, ResubscribeInterval = TimeSpan.Zero,
            SendTimeout = TimeSpan.FromMilliseconds(100), ReconnectPolicy = FastReconnectPolicy(),
        });
        var initial = Guid.NewGuid();
        client.EnqueueSubscribe(initial, CreateFilter(server.Endpoint));
        Assert.IsTrue(await WaitUntilAsync(() => client.GetSubscription(initial)?.State == SubscriptionState.Active));
        var connection = (RemoteEndpointConnection)typeof(RemoteEndpointClient)
            .GetField("connection", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)!;
        var sendGate = (SemaphoreSlim)typeof(RemoteEndpointConnection)
            .GetField("sendLock", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(connection)!;
        var failed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.ConnectionStatusChanged += (_, status) =>
        {
            if (status.EventType == ConnectionStatusEvent.Disconnected) failed.TrySetResult();
        };
        await sendGate.WaitAsync();
        var second = Guid.NewGuid();
        try
        {
            client.EnqueueSubscribe(second, CreateFilter(server.Endpoint, "/thing/second"));
            await failed.Task.WaitAsync(TimeSpan.FromSeconds(3));
        }
        finally { sendGate.Release(); }
        Assert.IsTrue(await WaitUntilAsync(() => client.GetSubscription(second)?.State == SubscriptionState.Active));
        var later = Guid.NewGuid();
        client.EnqueueSubscribe(later, CreateFilter(server.Endpoint, "/thing/later"));
        Assert.IsTrue(await WaitUntilAsync(() => client.GetSubscription(later)?.State == SubscriptionState.Active));
        Assert.IsFalse(((Task)typeof(RemoteEndpointConnection).GetField("refreshTask", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(connection)!).IsCompleted);
    }

    [TestMethod]
    public async Task SubscriptionPacing_DoesNotHoldConnectGateOrDelayOrderedLocalUpdates()
    {
        using var server = Phase6Server.Start(respondToPing: true);
        var delay = new ControlledDelayProvider();
        using var client = new RemoteEndpointClient(RemoteEndpointKey.Parse(server.Endpoint), new WebSocketCommunicationOptions
        {
            HeartbeatInterval = TimeSpan.Zero, HeartbeatTimeout = TimeSpan.Zero,
            SubscribeAckTimeout = TimeSpan.FromSeconds(30),
            ResubscribeInterval = TimeSpan.FromMilliseconds(50), DelayProvider = delay,
        });
        var first = Guid.NewGuid();
        var removed = Guid.NewGuid();
        client.EnqueueSubscribe(first, 0, CreateFilter(server.Endpoint));
        try
        {
            Assert.IsTrue(await WaitUntilAsync(() => delay.DelayCount > 0));
            var connection = (RemoteEndpointConnection)typeof(RemoteEndpointClient)
                .GetField("connection", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)!;
            var connectGate = (SemaphoreSlim)typeof(RemoteEndpointConnection)
                .GetField("connectLock", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(connection)!;
            Assert.AreEqual(1, connectGate.CurrentCount, "Pacing must not own the connection gate.");
            client.EnqueueSubscribe(removed, 0, CreateFilter(server.Endpoint, "/thing/removed"));
            client.EnqueueSubscribe(first, 1, CreateFilter(server.Endpoint, performanceLevel: 1));
            client.EnqueueUnsubscribe(removed);
            Assert.IsTrue(await WaitUntilAsync(() => client.GetSubscription(first)?.Revision == 1 &&
                client.GetSubscription(removed) == null && client.GetDiagnosticStatus().ActionQueueLength == 0));
            delay.ReleaseAll();
            Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Any(m => m.TokenId == first && m.Revision == 1)));
            await connection.EnsureConnectedAsync().WaitAsync(TimeSpan.FromSeconds(3));
            Assert.IsFalse(server.SubscribeMessages.Any(m => m.TokenId == removed));
            Assert.AreEqual(1, server.AcceptedCount);
            Assert.HasCount(2, server.SubscribeMessages.Where(m => m.TokenId == first).ToArray());
        }
        finally { delay.ReleaseAll(); }
    }

    [TestMethod]
    public async Task ConnectedCallback_CanEnsureConnectionAgainWithoutWaitingForItsOwnGate()
    {
        using var server = Phase6Server.Start(respondToPing: true);
        using var client = new RemoteEndpointClient(RemoteEndpointKey.Parse(server.Endpoint), new WebSocketCommunicationOptions
        {
            HeartbeatInterval = TimeSpan.Zero, HeartbeatTimeout = TimeSpan.Zero, ResubscribeInterval = TimeSpan.Zero,
        });
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.ConnectionStatusChanged += (_, status) =>
        {
            if (status.EventType == ConnectionStatusEvent.Connected)
                completed.TrySetResult(client.EnsureConnectedAsync().Wait(TimeSpan.FromSeconds(2)));
        };
        client.EnqueueSubscribe(Guid.NewGuid(), CreateFilter(server.Endpoint));
        Assert.IsTrue(await completed.Task.WaitAsync(TimeSpan.FromSeconds(4)));
        Assert.AreEqual(1, server.AcceptedCount);
        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 1));
    }

    [TestMethod]
    public async Task Heartbeat_ShouldSendPingAndAcceptPong()
    {
        using var server = Phase6Server.Start(respondToPing: true);
        using var module = CreateModule(heartbeatInterval: TimeSpan.FromMilliseconds(50), heartbeatTimeout: TimeSpan.FromSeconds(5));
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => server.PingCount > 0));
        await Task.Delay(120);
        Assert.AreEqual(RemoteConnectionState.Connected, GetOnlyConnection(module).State);
    }

    [TestMethod]
    public async Task Heartbeat_ShouldUseShorterServerNegotiatedInterval()
    {
        using var server = Phase6Server.Start(
            respondToPing: true,
            heartbeatIntervalMilliseconds: 30);
        using var module = CreateModule(
            heartbeatInterval: TimeSpan.FromSeconds(5),
            heartbeatTimeout: TimeSpan.FromSeconds(15));
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => server.PingCount > 0));
        Assert.AreEqual(
            TimeSpan.FromMilliseconds(30),
            GetOnlyConnection(module).EffectiveHeartbeatInterval);
    }

    [TestMethod]
    public async Task Heartbeat_ShouldUseShorterServerNegotiatedTimeout()
    {
        using var server = Phase6Server.Start(
            respondToPing: true,
            heartbeatIntervalMilliseconds: 30,
            heartbeatTimeoutMilliseconds: 120);
        using var module = CreateModule(
            heartbeatInterval: TimeSpan.FromMilliseconds(50),
            heartbeatTimeout: TimeSpan.FromMilliseconds(300));
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => server.PingCount > 0));
        Assert.AreEqual(
            TimeSpan.FromMilliseconds(120),
            GetOnlyConnection(module).EffectiveHeartbeatTimeout);
    }

    [TestMethod]
    public async Task HeartbeatTimeout_ShouldDisconnectReconnectAndResubscribe()
    {
        using var server = Phase6Server.Start(respondToPing: false);
        using var module = CreateModule(heartbeatInterval: TimeSpan.FromMilliseconds(40), heartbeatTimeout: TimeSpan.FromMilliseconds(120));
        var token = CreateToken();
        var statuses = new List<EventArg>();
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source == ConnectionStatusEvent.Source)
            {
                statuses.Add(arg);
            }
        });

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount >= 2, timeoutMilliseconds: 5000));
        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count(message => message.TokenId == GetMasterTokenIds(module, token.Id).Single()) >= 2));
        Assert.IsTrue(statuses.Any(arg => arg.EventType == ConnectionStatusEvent.Disconnected));
        Assert.IsTrue(statuses.Any(arg => arg.EventType == ConnectionStatusEvent.Reconnecting));
        Assert.IsGreaterThanOrEqualTo(
            statuses.Count(arg => arg.EventType == ConnectionStatusEvent.Connected),
            2);
        Assert.AreEqual(SubscriptionState.Active, GetSubscription(module, token.Id)?.State);
    }

    [TestMethod]
    public async Task ServerClose_ShouldReconnectAndResubscribeActiveToken()
    {
        using var server = Phase6Server.Start(respondToPing: true, closeFirstConnectionAfterSubscribe: true);
        var delayProvider = new RecordingDelayProvider();
        using var module = CreateModule(
            reconnectPolicy: new ReconnectPolicy
            {
                InitialDelay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromMilliseconds(1),
            },
            delayProvider: delayProvider);
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount >= 2, timeoutMilliseconds: 5000));
        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count(message => message.TokenId == GetMasterTokenIds(module, token.Id).Single()) >= 2));
        Assert.AreEqual(SubscriptionState.Active, GetSubscription(module, token.Id)?.State);
        Assert.IsTrue(delayProvider.Delays.Contains(TimeSpan.FromMilliseconds(1)));
    }

    [TestMethod]
    public async Task SendFailure_ShouldDropDisconnectedEventAndReceiveAfterReconnectWithoutDuplicatingSubscription()
    {
        using var server = Phase6Server.Start(respondToPing: true);
        using var module = CreateModule(reconnectPolicy: FastReconnectPolicy());
        var token = CreateToken();
        var receivedValues = new ConcurrentQueue<int>();
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source == "/thing/status")
            {
                receivedValues.Enqueue(arg.Sample.GetVal<int>());
            }
        });

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token.Id)?.State == SubscriptionState.Active));
        Assert.AreEqual(1, GetOnlyConnection(module).SubscriptionCount);
        var masterTokenId = GetMasterTokenIds(module, token.Id).Single();

        await server.SimulateSendFailureAsync("/thing/status", "Changed", 111);

        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount >= 2, 5000));
        Assert.IsTrue(await WaitUntilAsync(() =>
            server.SubscribeMessages.Count(message => message.TokenId == masterTokenId) >= 2,
            5000));
        Assert.IsTrue(await WaitUntilAsync(() =>
            GetOnlyConnection(module).State == RemoteConnectionState.Connected &&
            GetSubscription(module, token.Id)?.State == SubscriptionState.Active,
            5000));
        Assert.IsFalse(receivedValues.Contains(111));
        Assert.AreEqual(1, GetOnlyConnection(module).SubscriptionCount);

        await server.PublishAsync("/thing/status", "Changed", 222);

        Assert.IsTrue(await WaitUntilAsync(() => receivedValues.Contains(222), 5000));
        Assert.IsFalse(receivedValues.Contains(111));
    }

    [TestMethod]
    public async Task ResourceLikeSubscriptions_ShouldReconnectResubscribeAndReceiveAfterReconnect()
    {
        await VerifyResourceLikeReconnectAsync("/thing/NativeStatus");
        await VerifyResourceLikeReconnectAsync("/thing/ObservedStatus");
    }

    private async Task VerifyResourceLikeReconnectAsync(string source)
    {
        using var server = Phase6Server.Start(respondToPing: true);
        using var module = CreateModule(reconnectPolicy: FastReconnectPolicy());
        var token = CreateToken();
        var receivedValues = new ConcurrentQueue<int>();
        module.RegisterEventCallback(arg =>
        {
            if (string.Equals(arg.Source, source, StringComparison.OrdinalIgnoreCase))
            {
                receivedValues.Enqueue(arg.Sample.GetVal<int>());
            }
        });

        module.Subscribe(token, CreateFilter(server.Endpoint, source));

        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token.Id)?.State == SubscriptionState.Active));
        Assert.AreEqual(1, GetOnlyConnection(module).SubscriptionCount);
        var masterTokenId = GetMasterTokenIds(module, token.Id).Single();

        await server.SimulateSendFailureAsync(source, "Changed", 111);

        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount >= 2, 5000));
        Assert.IsTrue(await WaitUntilAsync(() =>
            server.SubscribeMessages.Count(message => message.TokenId == masterTokenId) >= 2,
            5000));
        Assert.IsTrue(await WaitUntilAsync(() =>
            GetOnlyConnection(module).State == RemoteConnectionState.Connected &&
            GetSubscription(module, token.Id)?.State == SubscriptionState.Active,
            5000));
        Assert.IsFalse(receivedValues.Contains(111));
        Assert.AreEqual(1, GetOnlyConnection(module).SubscriptionCount);

        await server.PublishAsync(source, "Changed", 222);

        Assert.IsTrue(await WaitUntilAsync(() => receivedValues.Contains(222), 5000));
        Assert.IsFalse(receivedValues.Contains(111));
    }

    [TestMethod]
    public async Task ReconnectAfterPerformanceLevelUpdate_ShouldSendLatestRevisionAndLevel()
    {
        using var server = Phase6Server.Start(respondToPing: true, closeFirstConnectionAfterSubscribe: true);
        var delayProvider = new ControlledDelayProvider();
        using var module = CreateModule(
            reconnectPolicy: FastReconnectPolicy(),
            delayProvider: delayProvider);
        var level1Token = CreateToken();
        var level0Token = CreateToken();

        module.Subscribe(level1Token, CreateFilter(server.Endpoint, performanceLevel: 1));
        Assert.IsTrue(await WaitUntilAsync(() =>
            server.SubscribeMessages.Count == 1 &&
            delayProvider.DelayCount > 0, 5000));

        module.Subscribe(level0Token, CreateFilter(server.Endpoint, performanceLevel: 0));
        Assert.IsTrue(await WaitUntilAsync(() =>
            GetSubscription(module, level1Token.Id)?.Revision == 1));

        delayProvider.ReleaseAll();

        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount >= 2, 5000));
        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count >= 2, 5000));
        var reconnectedSubscribe = server.SubscribeMessages.Last();
        Assert.AreEqual(server.SubscribeMessages[0].TokenId, reconnectedSubscribe.TokenId);
        Assert.AreEqual(1, reconnectedSubscribe.Revision);
        Assert.AreEqual(0, reconnectedSubscribe.Filter!.PerformanceLevel);
    }

    [TestMethod]
    public async Task InitialConnectionFailure_ShouldRemainPendingAndScheduleReconnect()
    {
        var delayProvider = new BlockingRecordingDelayProvider();
        using var module = CreateModule(delayProvider: delayProvider);
        var token = CreateToken();
        var endpoint = $"ws://127.0.0.1:{GetFreePort()}/cfet2/event/v2";

        module.Subscribe(token, CreateFilter(endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => delayProvider.Delays.Count > 0));
        Assert.AreEqual(RemoteConnectionState.Reconnecting, GetOnlyConnection(module).State);
        Assert.AreEqual(SubscriptionState.Pending, GetSubscription(module, token.Id)?.State);
        Assert.AreEqual(TimeSpan.FromSeconds(1), delayProvider.Delays[0]);
    }

    [TestMethod]
    public async Task PublisherInitiallyOffline_ShouldReconnectSubscribeAndReceiveEventAfterPublisherStarts()
    {
        var port = GetFreePort();
        var endpoint = $"ws://127.0.0.1:{port}/cfet2/event/v2";
        using var module = CreateModule(reconnectPolicy: FastReconnectPolicy());
        var token = CreateToken();
        var receivedEvents = new ConcurrentQueue<EventArg>();
        var statuses = new ConcurrentQueue<string>();
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source == "/thing/status")
            {
                receivedEvents.Enqueue(arg);
            }

            if (arg.Source == ConnectionStatusEvent.Source)
            {
                statuses.Enqueue(arg.EventType);
            }
        });

        module.Subscribe(token, CreateFilter(endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => statuses.Contains(ConnectionStatusEvent.Reconnecting)));
        Assert.AreEqual(SubscriptionState.Pending, GetSubscription(module, token.Id)?.State);

        using var server = Phase6Server.StartAtPort(port, respondToPing: true);
        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token.Id)?.State == SubscriptionState.Active, 5000));

        await server.PublishAsync("/thing/status", "Changed", 123);

        Assert.IsTrue(await WaitUntilAsync(() => receivedEvents.Any(), 5000));
        Assert.AreEqual(123, receivedEvents.Single().Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task MultipleSubscriptionsToSameOfflineEndpoint_ShouldAllRecoverAndReceiveEvents()
    {
        var port = GetFreePort();
        var endpoint = $"ws://127.0.0.1:{port}/cfet2/event/v2";
        using var module = CreateModule(reconnectPolicy: FastReconnectPolicy());
        var tokenA = CreateToken();
        var tokenB = CreateToken();
        var receivedSources = new ConcurrentQueue<string>();
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source.StartsWith("/thing/", StringComparison.Ordinal))
            {
                receivedSources.Enqueue(arg.Source);
            }
        });

        module.Subscribe(tokenA, CreateFilter(endpoint, "/thing/a"));
        module.Subscribe(tokenB, CreateFilter(endpoint, "/thing/b"));

        Assert.IsTrue(await WaitUntilAsync(() =>
            GetConnections(module).Count == 1 &&
            GetOnlyConnection(module).SubscriptionCount == 2 &&
            GetOnlyConnection(module).State != RemoteConnectionState.Failed));

        using var server = Phase6Server.StartAtPort(port, respondToPing: true);
        Assert.IsTrue(await WaitUntilAsync(() =>
            GetSubscription(module, tokenA.Id)?.State == SubscriptionState.Active &&
            GetSubscription(module, tokenB.Id)?.State == SubscriptionState.Active, 5000));

        await server.PublishAsync("/thing/a", "Changed", 1);
        await server.PublishAsync("/thing/b", "Changed", 2);

        Assert.IsTrue(await WaitUntilAsync(() =>
            receivedSources.Contains("/thing/a") &&
            receivedSources.Contains("/thing/b"), 5000));
        Assert.AreEqual(1, server.AcceptedCount);
        Assert.HasCount(2, server.SubscribeMessages);
    }

    [TestMethod]
    public async Task CancelLastSubscriptionDuringReconnect_WithDefaultIdleDelay_ShouldNotReconnect()
    {
        var port = GetFreePort();
        var endpoint = $"ws://127.0.0.1:{port}/cfet2/event/v2";
        var delayProvider = new ControlledDelayProvider();
        using var module = CreateModule(
            reconnectPolicy: FastReconnectPolicy(),
            delayProvider: delayProvider);
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(endpoint));
        Assert.IsTrue(await WaitUntilAsync(() => delayProvider.DelayCount > 0));

        module.UnSubScribe(token);
        Assert.IsTrue(await WaitUntilAsync(() => GetOnlyConnection(module).SubscriptionCount == 0));

        using var server = Phase6Server.StartAtPort(port, respondToPing: true);
        delayProvider.ReleaseAll();
        await Task.Delay(300);

        Assert.AreEqual(0, server.AcceptedCount);
        Assert.HasCount(1, GetConnections(module));
    }

    [TestMethod]
    public async Task ServerSendFailure_ShouldReconnectResubscribeAndContinueReceivingEvents()
    {
        using var server = Phase6Server.Start(respondToPing: true);
        using var module = CreateModule(reconnectPolicy: FastReconnectPolicy());
        var token = CreateToken();
        var receivedValues = new ConcurrentQueue<int>();
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source == "/thing/status")
            {
                receivedValues.Enqueue(arg.Sample.GetVal<int>());
            }
        });

        module.Subscribe(token, CreateFilter(server.Endpoint));
        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token.Id)?.State == SubscriptionState.Active));

        await server.SimulateSendFailureAsync("/thing/status", "Changed", -1);

        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount >= 2, 5000));
        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count >= 2, 5000));
        Assert.IsTrue(await WaitUntilAsync(
            () => GetSubscription(module, token.Id)?.State == SubscriptionState.Active,
            5000));

        await server.PublishAsync("/thing/status", "Changed", 456);

        Assert.IsTrue(await WaitUntilAsync(() => receivedValues.Contains(456), 5000));
        Assert.IsFalse(receivedValues.Contains(-1));
    }

    [TestMethod]
    public async Task ConnectionFailures_ShouldClassifyTransientAsRetryableAndHandshakeRejectionAsPermanent()
    {
        var transientPort = GetFreePort();
        var transientEndpoint = $"ws://127.0.0.1:{transientPort}/cfet2/event/v2";
        using var transientModule = CreateModule(reconnectPolicy: FastReconnectPolicy());
        var transientToken = CreateToken();
        var transientStatuses = new ConcurrentQueue<string>();
        transientModule.RegisterEventCallback(arg =>
        {
            if (arg.Source == ConnectionStatusEvent.Source)
            {
                transientStatuses.Enqueue(arg.EventType);
            }
        });

        transientModule.Subscribe(transientToken, CreateFilter(transientEndpoint));

        Assert.IsTrue(await WaitUntilAsync(() =>
            transientStatuses.Contains(ConnectionStatusEvent.Reconnecting)));
        Assert.IsFalse(transientStatuses.Contains(ConnectionStatusEvent.Failed));
        Assert.AreEqual(SubscriptionState.Pending, GetSubscription(transientModule, transientToken.Id)?.State);

        using var permanentServer = Phase6Server.StartRejectHello("permanent rejection");
        using var permanentModule = CreateModule(reconnectPolicy: FastReconnectPolicy());
        var permanentToken = CreateToken();

        permanentModule.Subscribe(permanentToken, CreateFilter(permanentServer.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() =>
            GetOnlyConnection(permanentModule).State == RemoteConnectionState.Failed));
        Assert.AreEqual(SubscriptionState.Failed, GetSubscription(permanentModule, permanentToken.Id)?.State);
        await Task.Delay(250);
        Assert.AreEqual(1, permanentServer.AcceptedCount);
    }

    [TestMethod]
    public async Task OldConnectionCloseAfterReconnect_ShouldNotOverwriteConnectedState()
    {
        using var server = Phase6Server.StartCloseFirstConnectionAfterSecondSubscribe();
        using var module = CreateModule(heartbeatInterval: TimeSpan.FromMilliseconds(40), heartbeatTimeout: TimeSpan.FromMilliseconds(120));
        var token = CreateToken();
        var statuses = new List<EventArg>();
        module.RegisterEventCallback(arg =>
        {
            if (arg.Source == ConnectionStatusEvent.Source)
            {
                statuses.Add(arg);
            }
        });

        module.Subscribe(token, CreateFilter(server.Endpoint));

        Assert.IsTrue(await WaitUntilAsync(() => server.AcceptedCount >= 2, timeoutMilliseconds: 5000));
        Assert.IsTrue(await WaitUntilAsync(() => server.FirstConnectionClosedAfterSecondSubscribe, timeoutMilliseconds: 5000));
        Assert.IsTrue(await WaitUntilAsync(() => GetOnlyConnection(module).State == RemoteConnectionState.Connected));

        Assert.AreEqual(RemoteConnectionState.Connected, GetOnlyConnection(module).State);
        Assert.IsGreaterThanOrEqualTo(statuses.Count(arg => arg.EventType == ConnectionStatusEvent.Connected), 2);
    }

    [TestMethod]
    public async Task CancelDuringReconnect_ShouldStopReconnectAndReleaseConnection()
    {
        using var server = Phase6Server.Start(respondToPing: true, closeFirstConnectionAfterSubscribe: true);
        using var module = CreateModule(idleReleaseDelay: TimeSpan.Zero);
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));
        Assert.IsTrue(await WaitUntilAsync(() => server.SubscribeMessages.Count == 1));

        module.UnSubScribe(token);

        Assert.IsTrue(await WaitUntilAsync(() => GetConnections(module).Count == 0));
        await Task.Delay(250);
        Assert.AreEqual(1, server.AcceptedCount);
    }

    [TestMethod]
    public async Task IdleRelease_ShouldReleaseConnectionAfterDelay()
    {
        using var server = Phase6Server.Start(respondToPing: true);
        using var module = CreateModule(idleReleaseDelay: TimeSpan.FromMilliseconds(120));
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));
        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token.Id)?.State == SubscriptionState.Active));

        module.UnSubScribe(token);

        Assert.IsNull(GetSubscription(module, token.Id));
        Assert.HasCount(1, GetConnections(module));
        Assert.IsTrue(await WaitUntilAsync(() => GetConnections(module).Count == 0));
    }

    [TestMethod]
    public async Task ImmediateIdleRelease_ShouldReleaseConnectionWithoutDelay()
    {
        using var server = Phase6Server.Start(respondToPing: true);
        using var module = CreateModule(releaseImmediately: true);
        var token = CreateToken();

        module.Subscribe(token, CreateFilter(server.Endpoint));
        Assert.IsTrue(await WaitUntilAsync(() => GetSubscription(module, token.Id)?.State == SubscriptionState.Active));

        module.UnSubScribe(token);

        Assert.IsTrue(await WaitUntilAsync(() => GetConnections(module).Count == 0));
    }

    private WebSocketCommunicationModule CreateModule(
        TimeSpan? heartbeatInterval = null,
        TimeSpan? heartbeatTimeout = null,
        TimeSpan? idleReleaseDelay = null,
        bool releaseImmediately = false,
        ReconnectPolicy? reconnectPolicy = null,
        IWebSocketDelayProvider? delayProvider = null)
    {
        return new WebSocketCommunicationModule(new WebSocketCommunicationOptions
        {
            ConnectTimeout = TimeSpan.FromSeconds(3),
            HandshakeTimeout = TimeSpan.FromSeconds(3),
            SendTimeout = TimeSpan.FromSeconds(3),
            ReceiveBufferSize = 1024,
            ResubscribeInterval = TimeSpan.Zero,
            HeartbeatInterval = heartbeatInterval ?? TimeSpan.FromSeconds(30),
            HeartbeatTimeout = heartbeatTimeout ?? TimeSpan.FromSeconds(90),
            IdleReleaseDelay = idleReleaseDelay ?? TimeSpan.FromSeconds(10),
            ReleaseConnectionImmediatelyWhenIdle = releaseImmediately,
            ReconnectPolicy = reconnectPolicy ?? new ReconnectPolicy(),
            DelayProvider = delayProvider ?? new SystemDelayProvider(),
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

    private static ReconnectPolicy FastReconnectPolicy()
    {
        return new ReconnectPolicy
        {
            InitialDelay = TimeSpan.FromMilliseconds(20),
            FastRetryCount = 1,
            MaxDelay = TimeSpan.FromMilliseconds(50),
        };
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

    private sealed class FirstPacingDelay : IWebSocketDelayProvider
    {
        internal static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(75);
        internal readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int pacingCount;
        internal bool Cancelled;
        public async Task DelayAsync(TimeSpan delay, CancellationToken token)
        {
            if (delay == Interval && Interlocked.Increment(ref pacingCount) == 1)
            {
                Entered.TrySetResult();
                try { await Task.Delay(Timeout.InfiniteTimeSpan, token); }
                catch (OperationCanceledException) { Cancelled = true; throw; }
            }
            else await Task.Delay(delay, token);
        }
    }

    private sealed class RecordingDelayProvider : IWebSocketDelayProvider
    {
        private readonly SystemDelayProvider inner = new();
        private readonly object gate = new();
        private readonly List<TimeSpan> delays = new();

        public IReadOnlyList<TimeSpan> Delays
        {
            get
            {
                lock (gate)
                {
                    return delays.ToArray();
                }
            }
        }

        public Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
        {
            lock (gate)
            {
                delays.Add(delay);
            }

            return inner.DelayAsync(delay, cancellationToken);
        }
    }

    private sealed class BlockingRecordingDelayProvider : IWebSocketDelayProvider
    {
        private readonly object gate = new();
        private readonly List<TimeSpan> delays = new();

        public IReadOnlyList<TimeSpan> Delays
        {
            get
            {
                lock (gate)
                {
                    return delays.ToArray();
                }
            }
        }

        public async Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
        {
            lock (gate)
            {
                delays.Add(delay);
            }

            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }
    }

    private sealed class ControlledDelayProvider : IWebSocketDelayProvider
    {
        private readonly TaskCompletionSource release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int delayCount;

        public int DelayCount => Volatile.Read(ref delayCount);

        public async Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref delayCount);
            await release.Task.WaitAsync(cancellationToken);
        }

        public void ReleaseAll()
        {
            release.TrySetResult();
        }
    }

    private sealed class Phase6Server : IDisposable
    {
        private readonly CancellationTokenSource cts = new();
        private readonly bool respondToPing;
        private readonly bool closeFirstConnectionAfterSubscribe;
        private readonly bool closeFirstConnectionAfterSecondSubscribe;
        private readonly string? rejectHelloReason;
        private readonly int heartbeatIntervalMilliseconds;
        private readonly int? heartbeatTimeoutMilliseconds;
        private readonly HttpListener listener;
        private readonly List<WebSocket> sockets = new();
        private readonly List<(WebSocket Socket, SubscribeMessage Message)> subscriptions = new();
        private readonly TaskCompletionSource firstSubscriptionReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly object gate = new();
        private readonly Task acceptLoop;

        private Phase6Server(
            bool respondToPing,
            bool closeFirstConnectionAfterSubscribe,
            bool closeFirstConnectionAfterSecondSubscribe,
            int heartbeatIntervalMilliseconds,
            int? heartbeatTimeoutMilliseconds,
            int? port = null,
            string? rejectHelloReason = null)
        {
            this.respondToPing = respondToPing;
            this.closeFirstConnectionAfterSubscribe = closeFirstConnectionAfterSubscribe;
            this.closeFirstConnectionAfterSecondSubscribe = closeFirstConnectionAfterSecondSubscribe;
            this.heartbeatIntervalMilliseconds = heartbeatIntervalMilliseconds;
            this.heartbeatTimeoutMilliseconds = heartbeatTimeoutMilliseconds;
            this.rejectHelloReason = rejectHelloReason;
            Port = port ?? GetFreePort();
            Endpoint = $"ws://127.0.0.1:{Port}/cfet2/event/v2";
            listener = new HttpListener();
            listener.Prefixes.Add($"http://127.0.0.1:{Port}/cfet2/event/v2/");
            listener.Start();
            acceptLoop = Task.Run(AcceptLoopAsync);
        }

        public string Endpoint { get; }

        public int Port { get; }

        public int AcceptedCount { get; private set; }

        public int PingCount { get; private set; }

        public bool FirstConnectionClosedAfterSecondSubscribe { get; private set; }

        public List<SubscribeMessage> SubscribeMessages { get; } = new();

        public Task FirstSubscriptionReceived => firstSubscriptionReceived.Task;

        public static Phase6Server Start(
            bool respondToPing,
            bool closeFirstConnectionAfterSubscribe = false,
            int heartbeatIntervalMilliseconds = 0,
            int? heartbeatTimeoutMilliseconds = null)
        {
            return new Phase6Server(
                respondToPing,
                closeFirstConnectionAfterSubscribe,
                closeFirstConnectionAfterSecondSubscribe: false,
                heartbeatIntervalMilliseconds,
                heartbeatTimeoutMilliseconds);
        }

        public static Phase6Server StartAtPort(int port, bool respondToPing)
        {
            return new Phase6Server(
                respondToPing,
                closeFirstConnectionAfterSubscribe: false,
                closeFirstConnectionAfterSecondSubscribe: false,
                heartbeatIntervalMilliseconds: 0,
                heartbeatTimeoutMilliseconds: null,
                port);
        }

        public static Phase6Server StartRejectHello(string reason)
        {
            return new Phase6Server(
                respondToPing: true,
                closeFirstConnectionAfterSubscribe: false,
                closeFirstConnectionAfterSecondSubscribe: false,
                heartbeatIntervalMilliseconds: 0,
                heartbeatTimeoutMilliseconds: null,
                rejectHelloReason: reason);
        }

        public static Phase6Server StartCloseFirstConnectionAfterSecondSubscribe()
        {
            return new Phase6Server(
                respondToPing: false,
                closeFirstConnectionAfterSubscribe: false,
                closeFirstConnectionAfterSecondSubscribe: true,
                heartbeatIntervalMilliseconds: 0,
                heartbeatTimeoutMilliseconds: null);
        }

        public async Task PublishAsync(string source, string eventType, int value)
        {
            (WebSocket Socket, SubscribeMessage Message)[] snapshot;
            lock (gate)
            {
                snapshot = subscriptions
                    .Where(subscription =>
                        subscription.Socket.State == WebSocketState.Open &&
                        subscription.Message.Filter?.SourcesAndTypes.Any(rule =>
                        string.Equals(rule.Source, source, StringComparison.OrdinalIgnoreCase) &&
                        string.Equals(rule.EventType, eventType, StringComparison.OrdinalIgnoreCase)) == true)
                    .ToArray();
            }

            foreach (var subscription in snapshot)
            {
                await SendAsync(subscription.Socket, new RemoteEventMessage
                {
                    TokenId = subscription.Message.TokenId,
                    EventArg = new EventArg(source, eventType, new Status<int>(value)),
                });
            }
        }

        public async Task SimulateSendFailureAsync(string source, string eventType, int value)
        {
            (WebSocket Socket, SubscribeMessage Message) subscription;
            lock (gate)
            {
                subscription = subscriptions.Last();
            }

            subscription.Socket.Abort();
            try
            {
                await SendAsync(subscription.Socket, new RemoteEventMessage
                {
                    TokenId = subscription.Message.TokenId,
                    EventArg = new EventArg(source, eventType, new Status<int>(value)),
                });
            }
            catch
            {
            }
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
            int connectionNumber;
            lock (gate)
            {
                AcceptedCount++;
                connectionNumber = AcceptedCount;
                sockets.Add(wsContext.WebSocket);
            }

            try
            {
                await ReceiveTextAsync(wsContext.WebSocket, cts.Token);
                await SendAsync(wsContext.WebSocket, new HelloAckMessage
                {
                    Accepted = rejectHelloReason == null,
                    Reason = rejectHelloReason,
                    HeartbeatIntervalMilliseconds = heartbeatIntervalMilliseconds,
                    HeartbeatTimeoutMilliseconds = heartbeatTimeoutMilliseconds,
                });
                if (rejectHelloReason != null)
                {
                    return;
                }

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
                                subscriptions.Add((wsContext.WebSocket, subscribe));
                                firstSubscriptionReceived.TrySetResult();
                            }

                            await SendAsync(wsContext.WebSocket, new SubscribeAckMessage
                            {
                                TokenId = subscribe.TokenId,
                                Revision = subscribe.Revision,
                                Accepted = true,
                            });

                            if (closeFirstConnectionAfterSubscribe && connectionNumber == 1)
                            {
                                await wsContext.WebSocket.CloseAsync(
                                    WebSocketCloseStatus.NormalClosure,
                                    "test reconnect",
                                    CancellationToken.None);
                                return;
                            }

                            if (closeFirstConnectionAfterSecondSubscribe && connectionNumber == 2)
                            {
                                WebSocket? firstSocket = null;
                                lock (gate)
                                {
                                    firstSocket = sockets.FirstOrDefault();
                                }

                                if (firstSocket != null)
                                {
                                    await firstSocket.CloseAsync(
                                        WebSocketCloseStatus.NormalClosure,
                                        "delayed old connection close",
                                        CancellationToken.None);
                                    FirstConnectionClosedAfterSecondSubscribe = true;
                                }
                            }

                            break;
                        case PingMessage:
                            lock (gate)
                            {
                                PingCount++;
                            }

                            if (respondToPing ||
                                closeFirstConnectionAfterSecondSubscribe && connectionNumber > 1)
                            {
                                await SendAsync(wsContext.WebSocket, new PongMessage());
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

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
