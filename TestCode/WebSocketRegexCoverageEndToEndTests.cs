using System.Collections.Concurrent;
using System.Reflection;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Attributes;
using Jtext103.CFET2.Core.Config;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions;

namespace TestCode;

public sealed partial class WebSocketServerPhase7Tests
{
    private static WebSocketSubscriptionManager Manager(WebSocketCommunicationModule module)
    {
        var server = typeof(WebSocketCommunicationModule).GetField("server", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(module)!;
        return (WebSocketSubscriptionManager)server.GetType().GetField("subscriptionManager", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(server)!;
    }

    [TestMethod]
    public async Task ReconnectWhileOldGetterIsBlockedKeepsCapacityAndCannotCommitOldSession()
    {
        var device = new RegexDevice { Block = true }; MyHub.TryAddThing(device, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen, maxPreparations: 1, maxPendingPreparations: 1);
        var manager = Manager(module); using var oldClient = await ConnectAndHelloAsync(listen);
        var id = Guid.NewGuid();
        try
        {
            await SendAsync(oldClient, CreateSubscribe(id, "/device/Temp.*", "changed", 0, 0));
            Assert.IsTrue(device.Entered.Wait(2000));
            oldClient.Abort();
            Assert.IsTrue(await WaitUntilAsync(() => module.GetServerSessions().Count == 0));
            using var newClient = await ConnectAndHelloAsync(listen);
            await SendAsync(newClient, CreateSubscribe(id, "/device/Temp.*", "changed", 0, 0));
            await SendAsync(newClient, new PingMessage()); await ReceiveUntilAsync<PongMessage>(newClient);
            Assert.IsFalse(device.Release.IsSet);
            Assert.AreEqual(1, manager.RunningPreparationCount); Assert.AreEqual(1, manager.QueuedPreparationCount);
            Assert.AreEqual(0, manager.RemoteSubscriptionCount); Assert.AreEqual(1, device.Reads);
            await SendAsync(newClient, CreateSubscribe(Guid.NewGuid(), "/device/Pres.*", "changed", 0, 0));
            Assert.IsFalse((await ReceiveUntilAsync<SubscribeAckMessage>(newClient)).Accepted);
            Assert.AreEqual(1, manager.RunningPreparationCount);
            device.Release.Set();
            var ack = await ReceiveUntilAsync<SubscribeAckMessage>(newClient);
            Assert.IsTrue(ack.Accepted); Assert.AreEqual(id, ack.TokenId);
            Assert.AreEqual(1, manager.RemoteSubscriptionCount);
            Assert.AreEqual(2, device.Reads, "Old cancelled initialization is cleaned before the new lifecycle reads.");
            await SendAsync(newClient, new UnsubscribeMessage { TokenId = id });
            Assert.IsTrue(await WaitUntilAsync(() => manager.RunningPreparationCount == 0 && GetObservedResourceCount() == 0));
        }
        finally { device.Release.Set(); }
    }

    [TestMethod]
    public async Task ClientSplitRulesCommitSuccessIndependentlyOfRejectedRuleAndReleaseBoth()
    {
        var device = new RegexDevice(); MyHub.TryAddThing(device, "/", "device");
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        var filter = new EventFilter("/device/Temp.*", "changed", 0, ToWebSocketUri(listen).ToString());
        filter.AddSourceAndType("/device/Broken", "changed");
        var events = new ConcurrentQueue<EventArg>();
        using var remote = MyHub.SubscribeResources(filter, events.Enqueue);
        Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().RemoteActiveSubscriptionCount == 1 && module.GetStatus().RemoteFailedSubscriptionCount == 1));
        Assert.AreEqual(1, Manager(module).RemoteSubscriptionCount); Assert.AreEqual(1, GetObservedResourceCount());
        device.Value = 12;
        Assert.IsTrue(await WaitUntilAsync(() => events.Count == 1));
        Assert.AreEqual(12, events.Single().Sample.GetVal<int>());
        remote.Dispose();
        Assert.IsTrue(await WaitUntilAsync(() => GetObservedResourceCount() == 0 && Manager(module).RemoteSubscriptionCount == 0
            && module.GetStatus().RemoteFailedSubscriptionCount == 0 && module.GetStatus().RemoteActiveSubscriptionCount == 0));
    }

    [TestMethod]
    public async Task RejectedUpdateKeepsOldServerEventsAndClientFailureSurvivesOldAck()
    {
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        using var client = await ConnectAndHelloAsync(listen); var id = Guid.NewGuid();
        var store = new RemoteEndpointSubscriptionStore(RemoteEndpointKey.Parse(ToWebSocketUri(listen).ToString()));
        var original = new EventFilter("/old/.*", "alarm");
        store.AddOrUpdate(id, 1, original);
        await SendAsync(client, new SubscribeMessage { TokenId = id, Revision = 1, Filter = original });
        var oldAck = await ReceiveUntilAsync<SubscribeAckMessage>(client); Assert.IsTrue(oldAck.Accepted);
        store.MarkSubscribeAck(oldAck); Assert.AreEqual(SubscriptionState.Active, store.Get(id)!.State);
        var bad = new EventFilter(); bad.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "[", EventType = "alarm" });
        store.AddOrUpdate(id, 2, bad);
        await SendAsync(client, new SubscribeMessage { TokenId = id, Revision = 2, Filter = bad });
        var rejection = await ReceiveUntilAsync<SubscribeAckMessage>(client); Assert.IsFalse(rejection.Accepted);
        store.MarkSubscribeAck(rejection); Assert.AreEqual(SubscriptionState.Failed, store.Get(id)!.State);
        store.MarkSubscribeAck(oldAck);
        Assert.AreEqual(2, store.Get(id)!.Revision); Assert.AreEqual(SubscriptionState.Failed, store.Get(id)!.State);
        Assert.AreEqual(rejection.Reason, store.Get(id)!.LastError);
        MyHub.Publish(new EventArg("/old/event", "alarm", new Status<int>(42)));
        Assert.AreEqual(42, (await ReceiveUntilAsync<RemoteEventMessage>(client)).EventArg!.Sample.GetVal<int>());
        await SendAsync(client, new UnsubscribeMessage { TokenId = id });
        Assert.IsTrue(await WaitUntilAsync(() => Manager(module).RemoteSubscriptionCount == 0 && GetLocalSubscriptionCount() == 0));
    }

    [TestMethod]
    public async Task ManyShortReadsHaveLongPreparationWithoutSlowGetAlarmAndKeepControlResponsive()
    {
        var field = typeof(Cfet2LogManager).GetField("provider", BindingFlags.Static | BindingFlags.NonPublic)!;
        var previous = field.GetValue(null); var threshold = GlobalConfig.ResourceSlowGetThresholdMs;
        var interval = GlobalConfig.ResourceObservationIntervalMs;
        var log = new CoverageLog(); var counter = new ReadCounter();
        try
        {
            field.SetValue(null, log); GlobalConfig.ResourceSlowGetThresholdMs = 500;
            Hub.KillMaster(); GlobalConfig.ResourceObservationIntervalMs = 60000; HubMaster.InjectHubToModule(this);
            for (int i = 0; i < 50; i++) MyHub.TryAddThing(new ShortReadDevice(counter), "/", "batch" + i);
            var listen = CreateListenUri(); using var module = StartServerModule(listen);
            using var client = await ConnectAndHelloAsync(listen); var id = Guid.NewGuid();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            await SendAsync(client, CreateSubscribe(id, "/batch.*/Value", "changed", 0, 0));
            Assert.IsTrue(await WaitUntilAsync(() => Volatile.Read(ref counter.Reads) >= 2));
            await SendAsync(client, new PingMessage());
            Assert.IsInstanceOfType<PongMessage>(await ReceiveAsync(client), "Ping must finish before the long batch ACK.");
            using var other = await ConnectAndHelloAsync(listen);
            await SubscribeAsync(other, Guid.NewGuid(), "/unregistered/.*", "alarm");
            await SendAsync(client, new UnsubscribeMessage { TokenId = id });
            await SendAsync(client, new PingMessage()); await ReceiveUntilAsync<PongMessage>(client);
            Assert.IsTrue(await WaitUntilAsync(() => Manager(module).RunningPreparationCount == 0 && GetObservedResourceCount() == 0));
            Assert.IsTrue(watch.ElapsedMilliseconds >= 500, "The batch exceeds a single-read threshold.");
            Assert.AreEqual(50, counter.Reads);
            Assert.IsFalse(log.Errors.Any(x => x.Contains("Get slow:") && x.Contains("/batch")));
            await SendAsync(client, new PingMessage());
            Assert.IsInstanceOfType<PongMessage>(await ReceiveAsync(client), "Cancelled batch must not send a late ACK.");
        }
        finally { field.SetValue(null, previous); GlobalConfig.ResourceSlowGetThresholdMs = threshold; GlobalConfig.ResourceObservationIntervalMs = interval; }
    }

    private sealed class ReadCounter { public int Reads; }
    private sealed class ShortReadDevice(ReadCounter counter) : Thing
    {
        [Cfet2Status] public int Value { get { Interlocked.Increment(ref counter.Reads); Thread.Sleep(20); return 1; } }
    }
    private sealed class CoverageLog : ICfet2LogProvider, ICfet2Logger
    {
        public readonly ConcurrentQueue<string> Errors = new();
        public ICfet2Logger GetLogger(string name) => this;
        public void StartLogCleanup() { }
        public void StopLogCleanup() { }
        public void Trace(string message) { }
        public void Debug(string message) { }
        public void Info(string message) { }
        public void Warn(string message) { }
        public void Error(string message) => Errors.Enqueue(message);
        public void Fatal(string message) { }
    }
}
