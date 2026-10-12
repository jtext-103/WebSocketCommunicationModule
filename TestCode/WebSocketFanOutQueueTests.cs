using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Reflection;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Jtext103.CFET2.WebSocketCommunicationModule.Server;
using Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed class WebSocketFanOutQueueTests : CFET2Host
{
    private ICfet2Logger previous = null!;
    private readonly QueueLogger logger = new();
    [TestInitialize]
    public void Init()
    {
        previous = WebSocketLog.Logger;
        WebSocketLog.Initialize(logger);
        KillHubIfAlive();
        HubMaster.InjectHubToModule(this);
    }
    [TestCleanup]
    public void Cleanup() { KillHubIfAlive(); WebSocketLog.Initialize(previous); }

    private static void KillHubIfAlive() { try { Hub.KillMaster(); } catch { } }

    [TestMethod]
    public async Task Capacity_DropsNewTasks_AcceptsOneFreeSlot_AndRecoversAtHalf()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1, 1, maxPendingFanOutEvents: 4);
        using var socket = new GateSocket();
        using var session = Session(manager, socket);
        var group = Subscribe(manager, session);
        Enqueue(manager, group, 0);
        await Until(() => socket.Started == 1);
        for (var i = 1; i <= 4; i++) Enqueue(manager, group, i);
        Enqueue(manager, group, 99);
        Enqueue(manager, group, 100);
        Assert.AreEqual(1, manager.ActiveFanOutEventCount);
        Assert.AreEqual(4, manager.PendingFanOutEventCount);
        Assert.AreEqual(2L, manager.FanOutQueueFullDropCount);
        Assert.AreEqual(1, logger.Messages.Count(m => m.Contains("fan-out overload")));
        socket.Release();
        await Until(() => socket.Started == 2);
        Assert.AreEqual(3, manager.PendingFanOutEventCount);
        Assert.IsFalse(logger.Messages.Any(m => m.Contains("fan-out recovered")));
        Enqueue(manager, group, 5); // An empty slot is usable before the half-capacity threshold.
        Assert.AreEqual(4, manager.PendingFanOutEventCount);
        socket.Release(2);
        await Until(() => socket.Started == 4);
        Assert.AreEqual(2, manager.PendingFanOutEventCount);
        Assert.IsTrue(logger.Messages.Any(m => m.Contains("recovered") && m.Contains("droppedSinceLastReport=1")));
        Enqueue(manager, group, 6);
        Enqueue(manager, group, 7);
        Enqueue(manager, group, 101);
        Assert.AreEqual(3L, manager.FanOutQueueFullDropCount);
        // The recovery must not reset the monotonic log rate limiter.
        var overloadTimes = logger.OverloadTimes.ToArray();
        for (var i = 1; i < overloadTimes.Length; i++)
            Assert.IsTrue(Stopwatch.GetElapsedTime(overloadTimes[i - 1], overloadTimes[i]).TotalMilliseconds >= 990);
        socket.Release(20);
        await Until(() => manager.ActiveFanOutEventCount == 0 && manager.PendingFanOutEventCount == 0);
        CollectionAssert.AreEqual(Enumerable.Range(0, 8).ToArray(), socket.Values.ToArray());
        Assert.AreEqual(4, manager.PeakPendingFanOutEventCount);
    }

    [TestMethod]
    public async Task ConcurrentProducers_RespectCapacityAndCountEveryDrop()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1, 2, maxPendingFanOutEvents: 8);
        using var socket = new GateSocket();
        using var session = Session(manager, socket);
        var group = Subscribe(manager, session);
        Enqueue(manager, group, -1);
        Enqueue(manager, group, -2);
        await Until(() => manager.ActiveFanOutEventCount == 2);
        Parallel.For(0, 100, i => Enqueue(manager, group, i));
        Assert.AreEqual(8, manager.PendingFanOutEventCount);
        Assert.AreEqual(92L, manager.FanOutQueueFullDropCount);
        manager.Dispose();
        await manager.FanOutCompletion.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.AreEqual(8L, manager.FanOutShutdownDiscardCount);
        Assert.AreEqual(0, manager.PendingFanOutEventCount);
        Assert.AreEqual(0, manager.ActiveFanOutEventCount);
        Enqueue(manager, group, 200);
        Assert.AreEqual(92L, manager.FanOutQueueFullDropCount);
        Assert.IsTrue(logger.Messages.Any(m => m.Contains("fan-out closed") && m.Contains("droppedSinceLastReport=91")));
        Assert.IsFalse(logger.Messages.Any(m => m.Contains("network send failed")));
        Assert.AreEqual(0, socket.Aborts);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task QueuedTargets_DoNotReachNewOrReplacementSubscribers(bool replace)
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1, 1, maxPendingFanOutEvents: 4);
        using var socket = new GateSocket();
        using var session = Session(manager, socket);
        var oldToken = Guid.NewGuid();
        var group = Subscribe(manager, session, oldToken);
        Enqueue(manager, group, 0);
        await Until(() => socket.Started == 1);
        Enqueue(manager, group, 1);
        var newToken = replace ? oldToken : Guid.NewGuid();
        Subscribe(manager, session, newToken, replace ? 1 : 0);
        socket.Release(10);
        await Until(() => manager.ActiveFanOutEventCount == 0 && manager.PendingFanOutEventCount == 0);
        Assert.AreEqual(replace ? 1 : 2, socket.Messages.Count);
        Assert.IsFalse(socket.Messages.Any(m => m.TokenId == newToken && m.EventArg!.Sample.GetVal<int>() == 1));
        Enqueue(manager, group, 2);
        await Until(() => socket.Messages.Any(m => m.TokenId == newToken && m.EventArg!.Sample.GetVal<int>() == 2));
    }

    [TestMethod]
    public async Task RemovedAndRecreatedGroup_DoesNotInheritQueuedEvents()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1, 1, maxPendingFanOutEvents: 4);
        using var socket = new GateSocket();
        using var session = Session(manager, socket);
        var token = Guid.NewGuid();
        var group = Subscribe(manager, session, token);
        Enqueue(manager, group, 0);
        await Until(() => socket.Started == 1);
        Enqueue(manager, group, 1);
        manager.Unsubscribe(session.Id, token);
        var replacement = Subscribe(manager, session, Guid.NewGuid());
        Assert.AreNotSame(group, replacement);
        socket.Release(10);
        await Until(() => manager.ActiveFanOutEventCount == 0 && manager.PendingFanOutEventCount == 0);
        CollectionAssert.AreEqual(new[] { 0 }, socket.Values.ToArray());
        Enqueue(manager, replacement, 2);
        await Until(() => socket.Values.Contains(2));
    }

    [TestMethod]
    public async Task WaitingForSessionLock_RechecksSubscriptionBeforeSocketSend()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1, 2);
        using var socket = new GateSocket();
        using var session = Session(manager, socket);
        var token = Guid.NewGuid();
        var group = Subscribe(manager, session, token);
        Enqueue(manager, group, 0);
        await Until(() => socket.Started == 1);
        Enqueue(manager, group, 1);
        await Until(() => manager.ActiveFanOutEventCount == 2);
        manager.Unsubscribe(session.Id, token);
        socket.Release(10);
        await Until(() => manager.ActiveFanOutEventCount == 0);
        Assert.AreEqual(1, socket.Started);
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    public async Task ConsumerSurvivesSerializationFailure_AndPreservesPayloadTimestamp(int level)
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1, 1);
        using var socket = new GateSocket();
        socket.Release(10);
        using var session = Session(manager, socket);
        var group = Subscribe(manager, session, level: level);
        var cycle = new List<object>(); cycle.Add(cycle);
        EnqueueEvent(manager, group, new EventArg("/queue/1", "changed", new Status<object>(cycle)));
        var timestamp = new DateTimeOffset(2026, 10, 12, 3, 0, 0, TimeSpan.Zero);
        EnqueueEvent(manager, group, new EventArg("/queue/1", "changed", new Status<int>(7) { Timestamp = timestamp }));
        await Until(() => socket.Messages.Count == 1 && manager.ActiveFanOutEventCount == 0);
        Assert.AreEqual(7, socket.Messages.Single().EventArg!.Sample.GetVal<int>());
        Assert.AreEqual(timestamp, socket.Messages.Single().EventArg!.Sample.Timestamp);
        Assert.AreEqual(0L, manager.FanOutQueueFullDropCount);
        Assert.IsTrue(logger.Messages.Any(m => m.Contains("serialization failed")));
    }

    [TestMethod]
    public async Task ServerDisposal_CleansConsumersBeforeStartAndAfterFailedStart()
    {
        foreach (var failStart in new[] { false, true })
        {
            using var server = new WebSocketEventServer(new WebSocketCommunicationOptions(), MyHub);
            var manager = (WebSocketSubscriptionManager)typeof(WebSocketEventServer)
                .GetField("subscriptionManager", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(server)!;
            if (failStart) Assert.ThrowsExactly<InvalidOperationException>(server.Start);
            server.Dispose(); server.Dispose();
            await manager.FanOutCompletion.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.ThrowsExactly<ObjectDisposedException>(server.Start);
        }
    }

    [TestMethod]
    public async Task ShutdownTimeout_DefersCleanupUntilUncooperativeSendExits()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1, 1, maxPendingFanOutEvents: 1);
        using var socket = new GateSocket { IgnoreCancellation = true };
        using var session = Session(manager, socket);
        var group = Subscribe(manager, session);
        Enqueue(manager, group, 0);
        await Until(() => socket.Started == 1);
        var stopwatch = Stopwatch.StartNew();
        manager.Dispose();
        Assert.IsTrue(stopwatch.Elapsed < TimeSpan.FromSeconds(8));
        Assert.IsFalse(manager.FanOutCompletion.IsCompleted);
        Assert.AreEqual(1, logger.Messages.Count(m => m.Contains("shutdown timed out")));
        manager.Dispose();
        socket.Release();
        await manager.FanOutCompletion.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.IsFalse(logger.Messages.Any(m => m.Contains("consumers failed")));
    }

    [TestMethod]
    public async Task DiagnosticLayers_ExposeGroupTaskCounts_NotRecipientCounts()
    {
        var options = new WebSocketCommunicationOptions { MaxConcurrentFanOutEvents = 1, MaxPendingFanOutEvents = 2 };
        using var server = new WebSocketEventServer(options, MyHub);
        using var module = new WebSocketCommunicationModule(options);
        WebSocketLog.Initialize(logger);
        typeof(WebSocketCommunicationModule).GetField("server", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(module, server);
        var manager = (WebSocketSubscriptionManager)typeof(WebSocketEventServer)
            .GetField("subscriptionManager", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(server)!;
        using var socket = new GateSocket();
        using var session = Session(manager, socket);
        var first = Subscribe(manager, session);
        Subscribe(manager, session); // Two recipients, still one group task per enqueue.
        var second = Subscribe(manager, session, path: "/queue/1");
        Enqueue(manager, first, 0);
        await Until(() => manager.ActiveFanOutEventCount == 1);
        Enqueue(manager, first, 1);
        Enqueue(manager, second, 1); // The same source event targets two groups.
        Enqueue(manager, first, 2);
        Enqueue(manager, second, 2);
        var status = module.GetStatus();
        Assert.AreEqual(2, status.PendingFanOutEventCount);
        Assert.AreEqual(1, status.ActiveFanOutEventCount);
        Assert.AreEqual(2L, status.FanOutQueueFullDropCount);
        Assert.AreEqual(2, status.PeakPendingFanOutEventCount);
        Assert.AreEqual(2, status.MaxPendingFanOutEvents);
        Assert.AreEqual(1, status.MaxConcurrentFanOutEvents);
        StringAssert.Contains(module.PrintStatus(), "fanOutDropped=2");
        server.Dispose();
        Assert.AreEqual(2L, module.GetStatus().FanOutShutdownDiscardCount);
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(-1)]
    public void InvalidCapacity_IsRejected(int capacity)
    {
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new WebSocketSubscriptionManager(MyHub, 1, maxPendingFanOutEvents: capacity));
    }

    [TestMethod]
    public async Task EnqueueRacingShutdown_DoesNotCountClosedRejectionsAsOverflow()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1, 1, maxPendingFanOutEvents: 2000);
        using var socket = new GateSocket();
        using var session = Session(manager, socket);
        var group = Subscribe(manager, session);
        Enqueue(manager, group, 0);
        await Until(() => socket.Started == 1);
        await Task.WhenAll(Task.Run(() => Parallel.For(0, 1000, i => Enqueue(manager, group, i))), Task.Run(manager.Dispose));
        Assert.AreEqual(0L, manager.FanOutQueueFullDropCount);
        Assert.AreEqual(0, manager.PendingFanOutEventCount);
        Assert.AreEqual(0, manager.ActiveFanOutEventCount);
        Assert.IsTrue(manager.FanOutCompletion.IsCompletedSuccessfully);
    }

    [TestMethod]
    public async Task ThrowingLogger_DoesNotKillConsumer()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1, 1, maxPendingFanOutEvents: 1);
        using var socket = new GateSocket();
        using var session = Session(manager, socket);
        var group = Subscribe(manager, session);
        Enqueue(manager, group, 0);
        await Until(() => socket.Started == 1);
        Enqueue(manager, group, 1);
        logger.Throw = true;
        Enqueue(manager, group, 2);
        socket.Release(10);
        await Until(() => manager.ActiveFanOutEventCount == 0 && manager.PendingFanOutEventCount == 0);
        Enqueue(manager, group, 3);
        await Until(() => socket.Values.Contains(3));
        Assert.AreEqual(1L, manager.FanOutQueueFullDropCount);
    }

    private WebSocketSession Session(WebSocketSubscriptionManager manager, GateSocket socket) =>
        new(new WebSocketCommunicationOptions { ServerSendTimeout = TimeSpan.FromSeconds(30) }, manager, socket);

    private AggregatedServerSubscription Subscribe(WebSocketSubscriptionManager manager, WebSocketSession session,
        Guid? token = null, int revision = 0, int level = 0, string path = "/queue/.*")
    {
        Assert.IsTrue(manager.Subscribe(session, token ?? Guid.NewGuid(), revision, new EventFilter(path, "changed", level)).Accepted);
        var groups = (ConcurrentDictionary<ServerSubscriptionKey, AggregatedServerSubscription>)typeof(WebSocketSubscriptionManager)
            .GetField("aggregatedSubscriptions", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!;
        return groups[new ServerSubscriptionKey(path, "changed")];
    }
    private static void Enqueue(WebSocketSubscriptionManager manager, AggregatedServerSubscription group, int value) =>
        EnqueueEvent(manager, group, new EventArg("/queue/1", "changed", new Status<int>(value)));
    private static void EnqueueEvent(WebSocketSubscriptionManager manager, AggregatedServerSubscription group, EventArg value) =>
        typeof(WebSocketSubscriptionManager).GetMethod("TryEnqueueFanOut", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(manager, [group, value]);
    private static async Task Until(Func<bool> predicate)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!predicate()) await Task.Delay(5, timeout.Token);
    }

    private sealed class GateSocket : WebSocket
    {
        private readonly SemaphoreSlim gate = new(0);
        private int started, aborts;
        private WebSocketState state = WebSocketState.Open;
        internal bool IgnoreCancellation;
        internal int Started => Volatile.Read(ref started);
        internal int Aborts => Volatile.Read(ref aborts);
        internal ConcurrentQueue<RemoteEventMessage> Messages { get; } = new();
        internal IEnumerable<int> Values => Messages.Select(m => m.EventArg!.Sample.GetVal<int>());
        internal void Release(int count = 1) => gate.Release(count);
        public override WebSocketCloseStatus? CloseStatus => null;
        public override string? CloseStatusDescription => null;
        public override string? SubProtocol => WebSocketProtocolConstants.SubProtocol;
        public override WebSocketState State => state;
        public override void Abort() { Interlocked.Increment(ref aborts); state = WebSocketState.Aborted; }
        public override void Dispose() { state = WebSocketState.Closed; }
        public override Task CloseAsync(WebSocketCloseStatus status, string? text, CancellationToken token) { Dispose(); return Task.CompletedTask; }
        public override Task CloseOutputAsync(WebSocketCloseStatus status, string? text, CancellationToken token) { Dispose(); return Task.CompletedTask; }
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken token) => throw new NotSupportedException();
        public override async Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType type, bool end, CancellationToken token)
        {
            Interlocked.Increment(ref started);
            await gate.WaitAsync(IgnoreCancellation ? CancellationToken.None : token);
            Messages.Enqueue((RemoteEventMessage)WebSocketMessageSerializer.DeserializeMessage(System.Text.Encoding.UTF8.GetString(buffer)));
        }
    }

    private sealed class QueueLogger : ICfet2Logger
    {
        internal bool Throw;
        internal ConcurrentQueue<string> Messages { get; } = new();
        internal ConcurrentQueue<long> OverloadTimes { get; } = new();
        public void Trace(string message) { }
        public void Debug(string message) { }
        public void Info(string message) { }
        public void Warn(string message) { }
        public void Error(string message)
        {
            if (Throw) throw new InvalidOperationException("Injected logger failure");
            Messages.Enqueue(message);
            if (message.Contains("fan-out overload")) OverloadTimes.Enqueue(Stopwatch.GetTimestamp());
        }
        public void Fatal(string message) { }
    }
}
