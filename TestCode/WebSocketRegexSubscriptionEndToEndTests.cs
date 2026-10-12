using System.Net.WebSockets;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Attributes;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

public sealed partial class WebSocketServerPhase7Tests
{
    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    public async Task RegexLoopbackTraversesClientAggregatorAndReturnsTimestampToHub(int level)
    {
        var d = new RegexDevice(); MyHub.TryAddThing(d, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        var events = new System.Collections.Concurrent.ConcurrentQueue<EventArg>();
        using var remote = MyHub.SubscribeResources(new EventFilter("/device/(Temperature|Pressure)", "changed", level, ToWebSocketUri(listen).ToString()), events.Enqueue);
        Assert.IsFalse(remote.HasLocalObservationResults);
        Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().RemoteActiveSubscriptionCount == 1));
        var changedAt = DateTimeOffset.UtcNow;
        d.Value = 73; d.Pressure = 29;
        Assert.IsTrue(await WaitUntilAsync(() => events.Count == 2));
        var received = events.ToDictionary(e => e.Source);
        Assert.AreEqual(73, received["/device/Temperature"].Sample.GetVal<int>());
        Assert.AreEqual(29, received["/device/Pressure"].Sample.GetVal<int>());
        Assert.IsTrue(received.Values.All(e => e.EventType == "changed" && e.Sample.IsRemote && e.Sample.Timestamp >= changedAt));
        remote.Dispose();
        Assert.IsTrue(await WaitUntilAsync(() => GetObservedResourceCount() == 0 && module.GetStatus().ServerRemoteSubscriptionCount == 0));
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task RegexActualClientAckTimeoutRetriesReusePreparation(bool cancelDuringRetry)
    {
        var d = new RegexDevice { Block = true }; MyHub.TryAddThing(d, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen, subscribeAckTimeout: TimeSpan.FromMilliseconds(100), maxSubscribeRetryCount: 20);
        using var remote = MyHub.SubscribeResources(new EventFilter("/device/Temp.*", "changed", 0, ToWebSocketUri(listen).ToString()), _ => { });
        try
        {
            Assert.IsTrue(d.Entered.Wait(2000));
            Assert.IsTrue(await WaitUntilAsync(() => module.GetRemoteClients().Any(c => c.Subscriptions.Any(s => s.SubscribeRetryCount >= 1))));
            Assert.AreEqual(1, d.Reads);
            if (cancelDuringRetry)
            {
                remote.Dispose();
                d.Release.Set();
                Assert.IsTrue(await WaitUntilAsync(() => d.Completed && GetObservedResourceCount() == 0 && GetLocalSubscriptionCount() == 0));
                return;
            }
            d.Release.Set();
            Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().RemoteActiveSubscriptionCount == 1));
            Assert.AreEqual(1, GetObservedResourceCount());
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task RegexCapacityRejectionStaysFailedAfterCapacityRecoversUntilExplicitResubscribe()
    {
        var d = new RegexDevice { Block = true }; MyHub.TryAddThing(d, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen,
            subscribeAckTimeout: TimeSpan.FromMilliseconds(80), maxPreparations: 1, maxPendingPreparations: 0);
        using var blocker = await ConnectAndHelloAsync(listen);
        var id = Guid.NewGuid();
        try
        {
            await SendAsync(blocker, CreateSubscribe(id, "/device/Temp.*", "changed", 0, 0));
            Assert.IsTrue(d.Entered.Wait(2000));
            var filter = new EventFilter("/device/Pres.*", "changed", 0, ToWebSocketUri(listen).ToString());
            using var remote = MyHub.SubscribeResources(filter, _ => { });
            Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().RemoteFailedSubscriptionCount == 1));
            StringAssert.Contains(module.GetRemoteClients().Single().Subscriptions.Single().LastError!, "Capacity");
            d.Release.Set(); await ReceiveUntilAsync<SubscribeAckMessage>(blocker);
            // Allow several ACK retry windows: an explicit rejection is not a retryable timeout.
            await Task.Delay(300);
            Assert.AreEqual(1, module.GetStatus().RemoteFailedSubscriptionCount);
            Assert.AreEqual(1, GetObservedResourceCount());
            remote.Dispose();
            using var retry = MyHub.SubscribeResources(filter, _ => { });
            Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().RemoteActiveSubscriptionCount == 1));
            Assert.AreEqual(2, GetObservedResourceCount());
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task RegexAckExhaustionReconnectsAndOldPreparationCannotCommitIntoNewSession()
    {
        var d = new RegexDevice { Block = true }; MyHub.TryAddThing(d, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen, subscribeAckTimeout: TimeSpan.FromMilliseconds(150), maxSubscribeRetryCount: 0);
        using var remote = MyHub.SubscribeResources(new EventFilter("/device/Temp.*", "changed", 0, ToWebSocketUri(listen).ToString()), _ => { });
        try
        {
            Assert.IsTrue(d.Entered.Wait(2000));
            var firstSession = module.GetServerSessions().Single().SessionId;
            Assert.IsTrue(await WaitUntilAsync(() => module.GetServerSessions().All(s => s.SessionId != firstSession), 4000));
            d.Release.Set();
            Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().RemoteActiveSubscriptionCount == 1, 6000));
            Assert.AreEqual(1, module.GetStatus().ServerRemoteSubscriptionCount);
            remote.Dispose();
            Assert.IsTrue(await WaitUntilAsync(() => GetObservedResourceCount() == 0 && GetLocalSubscriptionCount() == 0));
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task OverlappingRegexRealClientRecordsExistingDoubleDispatchBaseline()
    {
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        var host = ToWebSocketUri(listen).ToString();
        int a = 0, b = 0;
        using var first = MyHub.SubscribeResources(new EventFilter("/device/.*", "alarm", 0, host), _ => Interlocked.Increment(ref a));
        using var second = MyHub.SubscribeResources(new EventFilter("/device/Temp.*", "alarm", 0, host), _ => Interlocked.Increment(ref b));
        Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().RemoteActiveSubscriptionCount == 2));
        MyHub.Publish(new EventArg("/device/Temperature", "alarm", new Status<int>(1)));
        Assert.IsTrue(await WaitUntilAsync(() => a >= 2 && b >= 2));
        Assert.AreEqual(2, a); Assert.AreEqual(2, b);
        // Known pre-existing routing limitation: two wire tokens each re-enter all matching Hub filters.
        // This is a characterization, not a promise of exactly-once delivery.
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    public async Task RegexObservesMultipleResourcesAndForwardsActiveUnregisteredAndNativeEvents(int level)
    {
        var d = new RegexDevice(); MyHub.TryAddThing(d, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        using var client = await ConnectAndHelloAsync(listen); var id = Guid.NewGuid();
        await SubscribeAsync(client, id, "/device/.*", ".*", performanceLevel: level);
        Assert.AreEqual(2, GetObservedResourceCount());
        Assert.AreEqual(0, d.NativeReads);
        var group = (ResourceGroupSubscription)GetServerLocalSubscription(module, "/device/.*", ".*");
        Assert.IsTrue(group.ObservationResults.Any(r => r.Status == ResourceObservationStatus.Failed));
        Assert.IsTrue(group.ObservationResults.Any(r => r.ResourcePath.EndsWith("/Parameter") && r.Status == ResourceObservationStatus.Skipped));
        var start = DateTimeOffset.UtcNow;
        d.Value = 10; d.Pressure = 20;
        var seen = new HashSet<string>();
        for (int i = 0; i < 2; i++)
        {
            var message = await ReceiveUntilAsync<RemoteEventMessage>(client);
            Assert.AreEqual(id, message.TokenId);
            var source = level == 0 ? message.EventArg!.Source : message.Source;
            seen.Add(source!);
            var timestamp = level == 0 ? message.EventArg!.Sample.Timestamp : message.Timestamp;
            Assert.IsTrue(timestamp >= start);
        }
        CollectionAssert.AreEquivalent(new[] { "/device/Temperature", "/device/Pressure" }, seen.ToArray());
        foreach (var source in new[] { "/device/Native", "/device/custom/path", "/device/Broken", "/device/Parameter/1" })
        {
            MyHub.Publish(new EventArg(source, "alarm", new Status<int>(42)));
            var message = await ReceiveUntilAsync<RemoteEventMessage>(client);
            Assert.AreEqual(source, level == 0 ? message.EventArg!.Source : message.Source);
        }
        await SendAsync(client, new UnsubscribeMessage { TokenId = id });
        Assert.IsTrue(await WaitUntilAsync(() => GetObservedResourceCount() == 0 && GetLocalSubscriptionCount() == 0));
    }

    [TestMethod]
    public async Task RegexPreparationDoesNotBlockPingAndRetransmissionsDoNotReadAgain()
    {
        var d = new RegexDevice { Block = true }; MyHub.TryAddThing(d, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        using var client = await ConnectAndHelloAsync(listen); var id = Guid.NewGuid();
        try
        {
            await SendAsync(client, CreateSubscribe(id, "/device/Temp.*", "changed", 0, 0));
            Assert.IsTrue(d.Entered.Wait(2000));
            for (int i = 0; i < 5; i++) await SendAsync(client, CreateSubscribe(id, "/device/Temp.*", "changed", 0, 0));
            await SendAsync(client, new PingMessage());
            await ReceiveUntilAsync<PongMessage>(client);
            Assert.AreEqual(1, d.Reads);
            d.Release.Set(); Assert.IsTrue((await ReceiveUntilAsync<SubscribeAckMessage>(client)).Accepted);
            Assert.AreEqual(1, GetLocalSubscriptionCount());
            d.Value = 7;
            Assert.AreEqual(7, (await ReceiveUntilAsync<RemoteEventMessage>(client)).EventArg!.Sample.GetVal<int>());
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task SlowExactOrRegexCanBeUnsubscribedBeforeInitialReadReturns(bool exact)
    {
        var d = new RegexDevice { Block = true }; MyHub.TryAddThing(d, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        using var client = await ConnectAndHelloAsync(listen); var id = Guid.NewGuid();
        try
        {
            await SendAsync(client, CreateSubscribe(id, exact ? "/device/Temperature" : "/device/Temp.*", "changed", 0, 0));
            Assert.IsTrue(d.Entered.Wait(2000));
            await SendAsync(client, new UnsubscribeMessage { TokenId = id });
            await SendAsync(client, new PingMessage()); await ReceiveUntilAsync<PongMessage>(client);
            d.Release.Set();
            Assert.IsTrue(await WaitUntilAsync(() => d.Completed && GetObservedResourceCount() == 0 && GetLocalSubscriptionCount() == 0));
            await SendAsync(client, new PingMessage());
            Assert.IsInstanceOfType<PongMessage>(await ReceiveAsync(client), "No late success ACK after unsubscribe.");
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task RegexAlarmAndParameterizedPathsRemainEventOnlyAndPairedRulesDoNotCrossMatch()
    {
        var d = new RegexDevice(); MyHub.TryAddThing(d, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        using var client = await ConnectAndHelloAsync(listen); var id = Guid.NewGuid();
        var filter = new EventFilter("/device/.*", "alarm"); filter.AddSourceAndType("/other/.*", "changed");
        await SendAsync(client, new SubscribeMessage { TokenId = id, Filter = filter });
        Assert.IsTrue((await ReceiveUntilAsync<SubscribeAckMessage>(client)).Accepted);
        Assert.AreEqual(0, GetObservedResourceCount()); Assert.AreEqual(0, d.Reads);
        MyHub.Publish(new EventArg("/device/Temperature", "changed", new Status<int>(1)));
        MyHub.Publish(new EventArg("/other/a", "alarm", new Status<int>(2)));
        MyHub.Publish(new EventArg("/device/Temperature/1", "alarm", new Status<int>(3)));
        var message = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(3, message.EventArg!.Sample.GetVal<int>());
        await SendAsync(client, new UnsubscribeMessage { TokenId = id });
        await SubscribeAsync(client, Guid.NewGuid(), "/device/Temperature/.*", "changed");
        Assert.AreEqual(0, GetObservedResourceCount());
        MyHub.Publish(new EventArg("/device/Temperature/2", "changed", new Status<int>(4)));
        Assert.AreEqual(4, (await ReceiveUntilAsync<RemoteEventMessage>(client)).EventArg!.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task TwoSessionsShareRegexAndReconnectReusesSurvivingObservation()
    {
        var d = new RegexDevice(); MyHub.TryAddThing(d, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        using var a = await ConnectAndHelloAsync(listen); using var b = await ConnectAndHelloAsync(listen);
        var idA = Guid.NewGuid(); var idB = Guid.NewGuid();
        await SubscribeAsync(a, idA, "/device/Temp.*", "changed");
        await SubscribeAsync(b, idB, "/device/Temp.*", "changed");
        Assert.AreEqual(1, GetLocalSubscriptionCount()); Assert.AreEqual(1, GetObservedResourceCount());
        a.Abort(); Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 1));
        // Hold an existing poll so a reconnect cannot hide a baseline reset behind a later poll.
        d.Block = true;
        try
        {
            Assert.IsTrue(d.Entered.Wait(2000));
            var reads = d.Reads;
            var observation = GetServerLocalSubscription(module, "/device/Temp.*", "changed");
            d.Value = 9;
            using var reconnect = await ConnectAndHelloAsync(listen);
            await SubscribeAsync(reconnect, idA, "/device/Temp.*", "changed");
            Assert.AreSame(observation, GetServerLocalSubscription(module, "/device/Temp.*", "changed"));
            Assert.AreEqual(reads, d.Reads, "Rejoining must not perform an initial Get.");
            d.Block = false; d.Release.Set();
            var survivorEvent = await ReceiveUntilAsync<RemoteEventMessage>(b);
            var reconnectEvent = await ReceiveUntilAsync<RemoteEventMessage>(reconnect);
            Assert.AreEqual(idB, survivorEvent.TokenId); Assert.AreEqual(idA, reconnectEvent.TokenId);
            Assert.AreEqual(9, survivorEvent.EventArg!.Sample.GetVal<int>());
            Assert.AreEqual(9, reconnectEvent.EventArg!.Sample.GetVal<int>());
            await SendAsync(b, new UnsubscribeMessage { TokenId = idB });
            await SendAsync(reconnect, new UnsubscribeMessage { TokenId = idA });
            Assert.IsTrue(await WaitUntilAsync(() => GetObservedResourceCount() == 0));
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task OverlappingRegexWireBaselineSendsOneMessageForEachRemoteToken()
    {
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        using var client = await ConnectAndHelloAsync(listen); var a = Guid.NewGuid(); var b = Guid.NewGuid();
        await SubscribeAsync(client, a, "/device/.*", "alarm");
        await SubscribeAsync(client, b, "/device/Temp.*", "alarm");
        MyHub.Publish(new EventArg("/device/Temperature", "alarm", new Status<int>(42)));
        var first = await ReceiveUntilAsync<RemoteEventMessage>(client);
        var second = await ReceiveUntilAsync<RemoteEventMessage>(client);
        CollectionAssert.AreEquivalent(new[] { a, b }, new[] { first.TokenId!.Value, second.TokenId!.Value });
    }

    private sealed class RegexDevice : Thing
    {
        public volatile int Value = 1;
        public int Reads, NativeReads;
        public bool Block;
        public volatile bool Completed;
        public ManualResetEventSlim Entered = new(), Release = new();
        [Cfet2Status] public int Temperature
        {
            get
            {
                Interlocked.Increment(ref Reads);
                if (Block) { Entered.Set(); if (!Release.Wait(10000)) throw new TimeoutException("Test gate"); }
                Completed = true; return Value;
            }
        }
        [Cfet2Config] public int Pressure { get; set; } = 2;
        [Cfet2Status, Cfet2Event] public int Native { get { Interlocked.Increment(ref NativeReads); return 3; } }
        [Cfet2Status] public int Broken => throw new InvalidOperationException("unreadable");
        [Cfet2Status] public int Parameter(int channel) => channel;
        [Cfet2Status] public object[] Complex => [1];
    }
}
