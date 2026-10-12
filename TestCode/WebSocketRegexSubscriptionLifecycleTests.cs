using System.Collections;
using System.Net.WebSockets;
using System.Reflection;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Attributes;
using Jtext103.CFET2.Core.Config;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Server;
using Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions;

namespace TestCode;

[TestClass, DoNotParallelize]
public sealed partial class WebSocketRegexSubscriptionLifecycleTests : CFET2Host
{
    private readonly List<Device> devices = new();
    [TestInitialize] public void Init()
    {
        try { Hub.KillMaster(); } catch { }
        GlobalConfig.ResourceObservationIntervalMs = 10000;
        HubMaster.InjectHubToModule(this);
    }
    [TestCleanup] public void Cleanup()
    {
        foreach (var d in devices) d.Release.Set();
        Hub.KillMaster(); GlobalConfig.ResourceObservationIntervalMs = 500;
    }
    private Device Add(string name = "device", bool block = false)
    {
        var d = new Device { Block = block }; devices.Add(d); MyHub.TryAddThing(d, "/", name); return d;
    }
    private static WebSocketSession Session(WebSocketSubscriptionManager manager) => new(new WebSocketCommunicationOptions(), manager, new SinkSocket());
    private int Observed => (int)typeof(Hub).GetProperty("ObservedResourceCount", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(MyHub)!;
    private int Events => ((IDictionary)typeof(EventHub).GetProperty("localSubDict", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(MyHub.EventHub)!).Count;
    private static Task<ServerSubscribeResult> Subscribe(WebSocketSubscriptionManager m, WebSocketSession s, Guid id,
        string source = "/device/.*", int revision = 0, string type = "changed", int level = 0) =>
        m.SubscribeAsync(s, id, revision, new EventFilter(source, type, level));
    private static async Task Eventually(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(4000);
        while (!condition()) await Task.Delay(10, timeout.Token);
    }
    private static async Task Accepted(Task<ServerSubscribeResult> task) => Assert.IsTrue((await task.WaitAsync(TimeSpan.FromSeconds(4))).Accepted);

    [TestMethod]
    public async Task RegexGroupsShareHandlesWithLocalAndExactSubscriptionsAndReleaseIndependently()
    {
        var d = Add(); using var m = new WebSocketSubscriptionManager(MyHub, 1);
        using var a = Session(m); using var b = Session(m);
        var id = Guid.NewGuid(); var idB = Guid.NewGuid(); var overlap = Guid.NewGuid();
        await Accepted(Subscribe(m, a, id));
        await Accepted(Subscribe(m, b, idB));
        Assert.AreEqual(1, d.Reads); Assert.AreEqual(2, Observed); Assert.AreEqual(1, m.AggregatedSubscriptionCount);
        await Accepted(Subscribe(m, a, overlap, "/device/Val.*"));
        using var local = MyHub.SubscribeResources(new EventFilter("/device/Val.*", "changed"), _ => { });
        await Accepted(Subscribe(m, b, Guid.NewGuid(), "/device/Value"));
        Assert.AreEqual(2, d.Reads, "New exact subscription retains its initial Get contract.");
        m.Unsubscribe(a.Id, id); Assert.AreEqual(2, Observed);
        m.Unsubscribe(b.Id, idB); Assert.AreEqual(1, Observed);
        m.UnsubscribeSession(a.Id); m.UnsubscribeSession(b.Id);
        Assert.AreEqual(1, Observed); Assert.AreEqual(0, m.AggregatedSubscriptionCount);
        local.Dispose(); Assert.AreEqual(0, Observed); Assert.AreEqual(0, Events);
    }

    [TestMethod]
    public async Task PartialFailureSkippedAndZeroMatchesDoNotRetryOrRejectEventSubscription()
    {
        var d = Add(); d.Fail = true;
        using var m = new WebSocketSubscriptionManager(MyHub, 1); using var s = Session(m);
        var id = Guid.NewGuid();
        await Accepted(Subscribe(m, s, id, "/device/.*"));
        Assert.AreEqual(1, d.Reads); Assert.AreEqual(1, Observed);
        d.Fail = false;
        await Accepted(Subscribe(m, s, id, revision: 1, level: 1));
        await Accepted(Subscribe(m, s, Guid.NewGuid()));
        Assert.AreEqual(1, d.Reads);
        await Accepted(Subscribe(m, s, Guid.NewGuid(), "/device/Value/.*"));
        await Accepted(Subscribe(m, s, Guid.NewGuid(), "/device/.*", type: "alarm"));
        Assert.AreEqual(1, Observed);
        m.UnsubscribeSession(s.Id); Assert.AreEqual(0, Observed); Assert.AreEqual(0, Events);
        await Accepted(Subscribe(m, s, id)); Assert.AreEqual(2, d.Reads);
    }

    [TestMethod]
    public async Task DuplicateInFlightRequestsShareTaskAndCanceledWaiterDoesNotCancelOtherSession()
    {
        var d = Add(block: true); using var m = new WebSocketSubscriptionManager(MyHub, 1);
        using var a = Session(m); using var b = Session(m); var id = Guid.NewGuid();
        var first = Subscribe(m, a, id);
        try
        {
            Assert.IsTrue(d.Entered.Wait(2000));
            Assert.AreSame(first, Subscribe(m, a, id));
            var second = Subscribe(m, b, id);
            Assert.AreEqual(1, m.RunningPreparationCount); Assert.AreEqual(2, m.PendingRequestCount);
            m.Unsubscribe(a.Id, id); Assert.IsTrue(first.IsCanceled);
            d.Release.Set(); await Accepted(second);
            Assert.AreEqual(1, d.Reads); Assert.AreEqual(1, m.RemoteSubscriptionCount);
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task CanceledRunningGetterKeepsCapacityAndQueuedCancellationNeverReads()
    {
        var d = Add(block: true); var second = Add("second"); var third = Add("third");
        using var m = new WebSocketSubscriptionManager(MyHub, 1, 1, 1, 1); using var s = Session(m);
        var id = Guid.NewGuid(); var queuedId = Guid.NewGuid(); var rejectedId = Guid.NewGuid();
        var running = Subscribe(m, s, id);
        try
        {
            Assert.IsTrue(d.Entered.Wait(2000));
            var queued = Subscribe(m, s, queuedId, "/second/.*");
            var rejected = Subscribe(m, s, rejectedId, "/third/.*");
            Assert.AreEqual("subscriptionPreparationCapacityExceeded", (await rejected).Reason);
            m.Unsubscribe(s.Id, id); Assert.IsTrue(running.IsCanceled);
            Assert.AreEqual(1, m.RunningPreparationCount); Assert.AreEqual(0, second.Reads);
            m.Unsubscribe(s.Id, queuedId); Assert.IsTrue(queued.IsCanceled); Assert.AreEqual(0, m.QueuedPreparationCount);
            d.Release.Set(); await Eventually(() => m.RunningPreparationCount == 0);
            Assert.AreSame(rejected, Subscribe(m, s, rejectedId, "/third/.*"));
            Assert.AreEqual(0, third.Reads);
            await Accepted(Subscribe(m, s, rejectedId, "/third/.*", 1));
            Assert.AreEqual(0, second.Reads); Assert.AreEqual(1, third.Reads);
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task HigherFailedRevisionKeepsCommittedSubscriptionAndNeverRestoresOlderPreparation()
    {
        Add("old"); var d = Add(block: true);
        using var m = new WebSocketSubscriptionManager(MyHub, 1); using var s = Session(m); var id = Guid.NewGuid();
        await Accepted(Subscribe(m, s, id, "/old/.*", 1));
        var older = Subscribe(m, s, id, revision: 2);
        try
        {
            Assert.IsTrue(d.Entered.Wait(2000));
            var bad = new EventFilter(); bad.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "[", EventType = "changed" });
            var failed = m.SubscribeAsync(s, id, 3, bad);
            Assert.IsFalse((await failed).Accepted); Assert.IsTrue(older.IsCanceled);
            Assert.AreSame(failed, m.SubscribeAsync(s, id, 3, bad));
            Assert.AreEqual("staleRevision", (await Subscribe(m, s, id, "/old/.*", 2)).Reason);
            Assert.AreEqual("revisionConflict", (await Subscribe(m, s, id, "/old/.*", 3)).Reason);
            d.Release.Set(); await Eventually(() => m.RunningPreparationCount == 0);
            Assert.AreEqual(1, m.RemoteSubscriptionCount); Assert.AreEqual(1, m.AggregatedSubscriptionCount); Assert.AreEqual(2, Observed);
            await Accepted(Subscribe(m, s, id, "/old/.*", 4, level: 1));
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task MultiRuleFailureRollsBackOnlyNewGroupsAndPreservesSharedAndCommittedGroups()
    {
        Add(); using var m = new WebSocketSubscriptionManager(MyHub, 1, 1, 1, 4);
        using var s = Session(m); using var other = Session(m); var id = Guid.NewGuid();
        await Accepted(Subscribe(m, s, id, "/old/events", type: "alarm"));
        await Accepted(Subscribe(m, other, Guid.NewGuid(), "/device/Val.*"));
        var filter = new EventFilter("/device/Val.*", "changed");
        filter.AddSourceAndType("/device/Pres.*", "changed");
        filter.AddSourceAndType("/device/Action", "changed");
        Assert.IsFalse((await m.SubscribeAsync(s, id, 1, filter)).Accepted);
        await Eventually(() => m.RunningPreparationCount == 0);
        Assert.AreEqual(2, m.AggregatedSubscriptionCount); Assert.AreEqual(1, Observed); Assert.AreEqual(2, Events);
    }

    [TestMethod]
    public async Task DisposeDuringPreparationReturnsPromptlyAndCleansLateResources()
    {
        var d = Add(block: true); var m = new WebSocketSubscriptionManager(MyHub, 1); using var s = Session(m);
        var task = Subscribe(m, s, Guid.NewGuid());
        try
        {
            Assert.IsTrue(d.Entered.Wait(2000));
            await Task.Run(m.Dispose).WaitAsync(TimeSpan.FromSeconds(1));
            Assert.IsTrue(task.IsCanceled); Assert.AreEqual(1, m.RunningPreparationCount);
            d.Release.Set(); await Eventually(() => m.RunningPreparationCount == 0);
            Assert.AreEqual(0, Observed); Assert.AreEqual(0, Events); Assert.AreEqual(0, m.PendingRequestCount);
        }
        finally { d.Release.Set(); m.Dispose(); }
    }

    [TestMethod]
    public async Task UnsubscribeThenReuseTokenDoesNotLetOldWorkerRemoveNewSubscription()
    {
        var d = Add(block: true); Add("new"); using var m = new WebSocketSubscriptionManager(MyHub, 1);
        using var s = Session(m); var id = Guid.NewGuid(); var old = Subscribe(m, s, id);
        try
        {
            Assert.IsTrue(d.Entered.Wait(2000)); m.Unsubscribe(s.Id, id);
            await Accepted(Subscribe(m, s, id, "/new/.*"));
            d.Release.Set(); await Eventually(() => m.RunningPreparationCount == 0);
            Assert.IsTrue(old.IsCanceled); Assert.AreEqual(1, m.RemoteSubscriptionCount); Assert.AreEqual(2, Observed);
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task RegexCaseEscapesAndSignatureBoundariesRemainDistinctButLiteralCaseShares()
    {
        using var m = new WebSocketSubscriptionManager(MyHub, 1); using var s = Session(m);
        var id = Guid.NewGuid();
        await Accepted(Subscribe(m, s, id, @"/x/\d", type: @"\w"));
        Assert.AreEqual("revisionConflict", (await Subscribe(m, s, id, @"/x/\D", type: @"\w")).Reason);
        await Accepted(Subscribe(m, s, Guid.NewGuid(), @"/x/\D", type: @"\w"));
        await Accepted(Subscribe(m, s, Guid.NewGuid(), @"/x/\d", type: @"\W"));
        await Accepted(Subscribe(m, s, Guid.NewGuid(), "/literal", type: "alarm"));
        await Accepted(Subscribe(m, s, Guid.NewGuid(), "/LITERAL", type: "ALARM"));
        Assert.AreEqual(4, m.AggregatedSubscriptionCount);
        var f = new EventFilter("/a\0b", "c"); var g = new EventFilter("/a", "b\0c");
        var collision = Guid.NewGuid(); await Accepted(m.SubscribeAsync(s, collision, 0, f));
        Assert.AreEqual("revisionConflict", (await m.SubscribeAsync(s, collision, 0, g)).Reason);
        var order = Guid.NewGuid(); f = new EventFilter("/first", "alarm"); f.AddSourceAndType("/second", "alarm");
        g = new EventFilter("/SECOND", "ALARM"); g.AddSourceAndType("/FIRST", "ALARM");
        var first = m.SubscribeAsync(s, order, 0, f); await Accepted(first);
        Assert.AreSame(first, m.SubscribeAsync(s, order, 0, g));
    }

    [TestMethod]
    public void ClientDoesNotFoldRegexEscapesOrMergeSignatureSeparators()
    {
        var aggregator = new ClientSubscriptionAggregator();
        using var token = MyHub.Subscribe(new EventFilter("/token", "alarm"), _ => { });
        var filter = new EventFilter(@"/x/\d", @"\w", 0, "ws://127.0.0.1:12345/cfet2/event/v2");
        filter.AddSourceAndType(@"/x/\D", @"\w"); filter.AddSourceAndType(@"/x/\d", @"\W");
        filter.AddSourceAndType(@"/x/\d", @"\w");
        filter.AddSourceAndType("/a\0b", "c");
        filter.AddSourceAndType("/a", "b\0c");
        Assert.HasCount(5, aggregator.Subscribe(token, filter).Actions);
    }

    [TestMethod]
    public void InvalidCapacityIsRejected()
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => new WebSocketSubscriptionManager(MyHub, 1, 1, 0));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => new WebSocketSubscriptionManager(MyHub, 1, 1, 1, -1));
    }

    [TestMethod]
    public async Task PerformanceUpdateDuringPreparationTransfersReservationWithoutAnotherRead()
    {
        var d = Add(block: true); using var m = new WebSocketSubscriptionManager(MyHub, 1, 1, 1, 0);
        using var s = Session(m); var id = Guid.NewGuid(); var old = Subscribe(m, s, id);
        try
        {
            Assert.IsTrue(d.Entered.Wait(2000));
            var update = Subscribe(m, s, id, revision: 1, level: 1);
            Assert.IsTrue(old.IsCanceled); Assert.IsFalse(update.IsCompleted);
            Assert.AreEqual(1, m.RunningPreparationCount); Assert.AreEqual(0, m.QueuedPreparationCount);
            d.Release.Set(); await Accepted(update);
            Assert.AreEqual(1, d.Reads); Assert.AreEqual(1, m.RemoteSubscriptionCount);
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task ReadyGroupsRemainReusableWhenAllPreparationSlotsAreBlocked()
    {
        Add("ready"); var d = Add(block: true);
        using var m = new WebSocketSubscriptionManager(MyHub, 1, 1, 1, 0); using var s = Session(m);
        await Accepted(Subscribe(m, s, Guid.NewGuid(), "/ready/.*"));
        var slow = Subscribe(m, s, Guid.NewGuid());
        try
        {
            Assert.IsTrue(d.Entered.Wait(2000));
            await Accepted(Subscribe(m, s, Guid.NewGuid(), "/ready/.*"));
            Assert.IsFalse((await Subscribe(m, s, Guid.NewGuid())).Accepted, "Independent waiters are bounded too.");
            d.Release.Set(); await Accepted(slow);
        }
        finally { d.Release.Set(); }
    }

    [TestMethod]
    public async Task RegexInitialTimeoutCachesFailureAndPreservesPreviouslyCommittedSubscription()
    {
        Add(new string('a', 100) + "!"); using var m = new WebSocketSubscriptionManager(MyHub, 1); using var s = Session(m);
        var id = Guid.NewGuid(); await Accepted(Subscribe(m, s, id, "/old", type: "alarm"));
        var failed = Subscribe(m, s, id, "^/(a+)+$", 1);
        Assert.IsFalse((await failed.WaitAsync(TimeSpan.FromSeconds(3))).Accepted);
        Assert.AreSame(failed, Subscribe(m, s, id, "^/(a+)+$", 1));
        Assert.AreEqual(0, Observed); Assert.AreEqual(1, Events); Assert.AreEqual(1, m.RemoteSubscriptionCount);
    }

    [TestMethod]
    public async Task OldAggregateCallbackCannotReachReplacementAndCleanupContinuesAfterFailure()
    {
        using var log = new CleanupLog();
        using var m = new WebSocketSubscriptionManager(MyHub, 1); using var socket = new SinkSocket();
        using var s = new WebSocketSession(new WebSocketCommunicationOptions(), m, socket); var id = Guid.NewGuid();
        await Accepted(Subscribe(m, s, id, "/old", type: "alarm"));
        var groups = (System.Collections.Concurrent.ConcurrentDictionary<ServerSubscriptionKey, AggregatedServerSubscription>)
            typeof(WebSocketSubscriptionManager).GetField("aggregatedSubscriptions", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(m)!;
        var old = groups.Values.Single();
        m.Unsubscribe(s.Id, id);
        await Accepted(Subscribe(m, s, id, "/old", type: "alarm"));
        var replacement = groups.Values.Single(); Assert.AreNotSame(old, replacement); Assert.IsFalse(old.IsActive);
        var fanOut = typeof(WebSocketSubscriptionManager).GetMethod("TryEnqueueFanOut", BindingFlags.Instance | BindingFlags.NonPublic)!;
        fanOut.Invoke(m, [old, new EventArg("/old", "alarm", new Jtext103.CFET2.Core.Sample.Status<int>(1))]);
        Assert.AreEqual(0, socket.SendCount);
        Assert.AreEqual(0, m.PendingFanOutEventCount);
        fanOut.Invoke(m, [replacement, new EventArg("/old", "alarm", new Jtext103.CFET2.Core.Sample.Status<int>(2))]);
        await Eventually(() => socket.SendCount == 1 && m.ActiveFanOutEventCount == 0);
        Assert.AreEqual(1, socket.SendCount);
        Assert.AreEqual(0, m.ActiveFanOutEventCount);
        replacement.LocalSubscription = new ThrowAfterDispose(replacement.LocalSubscription!);
        await Accepted(Subscribe(m, s, Guid.NewGuid(), "/second", type: "alarm"));
        m.Dispose(); Assert.AreEqual(0, Events); Assert.AreEqual(0, m.RemoteSubscriptionCount);
        Assert.AreEqual(1, log.Errors.Count(x => x.Contains("Local subscription cleanup failed:") && x.Contains("Test release error")));
    }

    [TestMethod]
    public async Task CompletionCancellationRaceLeavesNoSubscriptionsOrReservations()
    {
        for (int i = 0; i < 12; i++)
        {
            var d = Add("race" + i, block: true);
            using var m = new WebSocketSubscriptionManager(MyHub, 1); using var s = Session(m);
            var id = Guid.NewGuid(); var task = Subscribe(m, s, id, "/race" + i + "/.*");
            try
            {
                Assert.IsTrue(d.Entered.Wait(2000));
                await Task.WhenAll(Task.Run(() => m.Unsubscribe(s.Id, id)), Task.Run(d.Release.Set));
                await Eventually(() => m.RunningPreparationCount == 0);
                Assert.IsTrue(task.IsCompleted); Assert.AreEqual(0, Events); Assert.AreEqual(0, Observed);
                Assert.AreEqual(0, m.PendingRequestCount); Assert.AreEqual(0, m.QueuedPreparationCount);
            }
            finally { d.Release.Set(); }
        }
    }

    private sealed class ThrowAfterDispose(IDisposable inner) : IDisposable
    { public void Dispose() { inner.Dispose(); throw new InvalidOperationException("Test release error"); } }

    [TestMethod]
    [DataRow("/parameter/Temperature", true, 1)]
    [DataRow("/parameter/Temp.*", true, 1)]
    [DataRow("/parameter/Temperature/.*", true, 0)]
    [DataRow("/parameter/Temperature/*", true, 1)]
    [DataRow("/parameter/Temperature/1", true, 0)]
    [DataRow("/parameter/Def.*", true, 1)]
    public async Task ParameterizedResourcesRespectExactAndRegexObservationBoundaries(string source, bool accepted, int results)
    {
        var device = new ParameterDevice(); MyHub.TryAddThing(device, "/", "parameter");
        using var m = new WebSocketSubscriptionManager(MyHub, 1); using var s = Session(m);
        Assert.AreEqual(accepted, (await Subscribe(m, s, Guid.NewGuid(), source)).Accepted);
        if (!accepted) { Assert.AreEqual(0, Events); return; }
        var groups = (System.Collections.Concurrent.ConcurrentDictionary<ServerSubscriptionKey, AggregatedServerSubscription>)
            typeof(WebSocketSubscriptionManager).GetField("aggregatedSubscriptions", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(m)!;
        var group = (ResourceGroupSubscription)groups.Values.Single().LocalSubscription!;
        Assert.AreEqual(results, group.ObservationResults.Count);
        if (results == 1)
            Assert.AreEqual(ResourceObservationStatus.Skipped,
                group.ObservationResults.Single().Status);
        Assert.AreEqual(0, Observed);
        Assert.AreEqual(-1, device.LastDefaultChannel);
    }

    private sealed class ParameterDevice : Thing
    {
        public int LastDefaultChannel = -1;
        [Cfet2Status] public int Temperature(int channel) => channel;
        [Cfet2Status] public int Default(int channel = 7) { LastDefaultChannel = channel; return channel; }
    }

    public sealed class Device : Thing
    {
        public bool Block, Fail;
        public int Reads;
        public ManualResetEventSlim Entered = new(), Release = new();
        [Cfet2Status] public int Value
        {
            get
            {
                Interlocked.Increment(ref Reads);
                if (Block) { Entered.Set(); if (!Release.Wait(10000)) throw new TimeoutException("Test release missing"); }
                if (Fail) throw new InvalidOperationException("Device failed");
                return 1;
            }
        }
        [Cfet2Config] public int Pressure { get; set; } = 2;
        [Cfet2Status, Cfet2Event] public int Native => 3;
        [Cfet2Status] public int Parameter(int channel) => channel;
        [Cfet2Status] public object[] Complex => [1];
        [Cfet2Method] public void Action() { }
    }
    private sealed class SinkSocket : WebSocket
    {
        public int SendCount;
        public override WebSocketCloseStatus? CloseStatus => null;
        public override string? CloseStatusDescription => null;
        public override WebSocketState State => WebSocketState.Open;
        public override string? SubProtocol => null;
        public override void Abort() { }
        public override void Dispose() { }
        public override Task CloseAsync(WebSocketCloseStatus s, string? d, CancellationToken c) => Task.CompletedTask;
        public override Task CloseOutputAsync(WebSocketCloseStatus s, string? d, CancellationToken c) => Task.CompletedTask;
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> b, CancellationToken c) => throw new NotSupportedException();
        public override Task SendAsync(ArraySegment<byte> b, WebSocketMessageType t, bool e, CancellationToken c)
        { Interlocked.Increment(ref SendCount); return Task.CompletedTask; }
    }
}
