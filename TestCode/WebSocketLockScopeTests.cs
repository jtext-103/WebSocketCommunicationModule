using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Reflection;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Jtext103.CFET2.WebSocketCommunicationModule.Server;
using Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions;

namespace TestCode;

[TestClass, DoNotParallelize]
public sealed class WebSocketLockScopeTests : CFET2Host
{
    private ICfet2Logger previousLogger = null!;
    [TestInitialize]
    public void Init()
    {
        try { Hub.KillMaster(); } catch { }
        HubMaster.InjectHubToModule(this);
        previousLogger = WebSocketLog.Logger;
        WebSocketLog.Initialize(WebSocketLog.EmptyLogger.Instance);
    }
    [TestCleanup]
    public void Cleanup() { Hub.KillMaster(); WebSocketLog.Initialize(previousLogger); }

    [TestMethod]
    public async Task ReconnectCallback_DoesNotHoldGate_AndConcurrentRequestsShareOneWorker()
    {
        var endpoint = RemoteEndpointKey.Parse("ws://127.0.0.1:1/cfet2/event/v2");
        var store = new RemoteEndpointSubscriptionStore(endpoint);
        store.AddOrUpdate(Guid.NewGuid(), new EventFilter("/locks/a", "changed"));
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var callbackHeldLock = true;
        RemoteEndpointConnection? connection = null;
        connection = new RemoteEndpointConnection(endpoint, new WebSocketCommunicationOptions
        {
            DelayProvider = new NeverDelay(),
        }, store, _ => { }, (type, _, _) =>
        {
            if (type != ConnectionStatusEvent.Reconnecting) return;
            callbackHeldLock = Monitor.IsEntered(Field<object>(connection!, "reconnectLock"));
            entered.Set();
            release.Wait(TimeSpan.FromSeconds(5));
        });
        using (connection)
        {
            var schedule = typeof(RemoteEndpointConnection).GetMethod("ScheduleReconnectIfNeeded", BindingFlags.NonPublic | BindingFlags.Instance)!;
            try
            {
                schedule.Invoke(connection, ["test"]);
                Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(3)));
                var worker = Field<Task>(connection, "reconnectTask");
                await Task.WhenAll(Enumerable.Range(0, 32).Select(_ => Task.Run(() => schedule.Invoke(connection, ["duplicate"]))))
                    .WaitAsync(TimeSpan.FromSeconds(2));
                Assert.IsFalse(callbackHeldLock);
                Assert.AreSame(worker, Field<Task>(connection, "reconnectTask"));
            }
            finally { release.Set(); }
        }
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    [DataRow(2)]
    public async Task SessionCleanup_InvalidatesSubscriptionBeforeDisposingOutsideLifecycleLock(int mode)
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var session = new WebSocketSession(new(), manager, new SinkSocket());
        var token = Guid.NewGuid();
        Assert.IsTrue((await manager.SubscribeAsync(session, token, 0, new EventFilter("/locks/a", "changed"))).Accepted);
        if (mode == 2)
            Assert.IsTrue((await manager.SubscribeAsync(session, Guid.NewGuid(), 0, new EventFilter("/locks/b", "changed"))).Accepted);
        var groups = Field<ConcurrentDictionary<ServerSubscriptionKey, AggregatedServerSubscription>>(manager, "aggregatedSubscriptions");
        var group = groups[new("/locks/a", "changed")];
        using var blocking = new BlockingCleanup(group.LocalSubscription!);
        group.LocalSubscription = blocking;
        var operation = Task.Run(async () =>
        {
            if (mode == 1) session.Dispose();
            else if (mode == 2) typeof(WebSocketSession).GetMethod("BeginSubscribe", BindingFlags.NonPublic | BindingFlags.Instance)!
                .Invoke(session, [new SubscribeMessage { TokenId = token, Revision = 1, Filter = new EventFilter("/locks/b", "changed") }, CancellationToken.None]);
            else await (Task)typeof(WebSocketSession).GetMethod("UnsubscribeAsync", BindingFlags.NonPublic | BindingFlags.Instance)!
                .Invoke(session, [token, CancellationToken.None])!;
        });
        try
        {
            Assert.IsTrue(blocking.Entered.Wait(TimeSpan.FromSeconds(3)));
            Assert.AreEqual(mode == 2 ? 2 : 0, manager.RemoteSubscriptionCount);
            Assert.IsFalse(group.IsActive);
            await Task.Run(() => { lock (Field<object>(session, "lifecycleLock")) { } })
                .WaitAsync(TimeSpan.FromSeconds(2));
        }
        finally { blocking.Release.Set(); await operation.WaitAsync(TimeSpan.FromSeconds(3)); }
    }

    [TestMethod]
    public async Task ReusedFailureLogger_DoesNotBlockOtherModuleSubscriptionOperations()
    {
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());
        WebSocketLog.Initialize(WebSocketLog.EmptyLogger.Instance);
        var first = new Token(MyHub.EventHub, "ws", "ws://127.0.0.1:1/cfet2/event/v2");
        var filter = new EventFilter("/locks/a", "changed", 0, "ws://127.0.0.1:1/cfet2/event/v2");
        module.Subscribe(first, filter);
        var client = WebSocketClientTestHelper.GetOnlyEndpointClient(module);
        var master = WebSocketClientTestHelper.GetMasterTokenIds(module, first.Id).Single();
        await Until(() => client.GetSubscription(master) != null);
        var store = Field<RemoteEndpointSubscriptionStore>(client, "subscriptionStore");
        store.MarkSubscribeAck(new() { TokenId = master, Accepted = false, Reason = "injected rejection" });
        using var logger = new BlockingReuseLogger();
        WebSocketLog.Initialize(logger);
        var reuse = Task.Run(() => module.Subscribe(first, filter));
        try
        {
            Assert.IsTrue(logger.Entered.Wait(TimeSpan.FromSeconds(3)));
            await Task.Run(() => module.UnSubScribe(first)).WaitAsync(TimeSpan.FromSeconds(2));
            Assert.AreEqual(0, module.GetStatus().LocalSubscriptionCount);
        }
        finally { logger.Release.Set(); await reuse.WaitAsync(TimeSpan.FromSeconds(3)); }
    }

    [TestMethod]
    public void LateSubscribeCompletion_CannotChangeReplacementOrReconnectAttempt()
    {
        var store = new RemoteEndpointSubscriptionStore(RemoteEndpointKey.Parse("ws://localhost:1/cfet2/event/v2"));
        var token = Guid.NewGuid();
        store.AddOrUpdate(token, 0, new EventFilter("/locks/a", "changed"));
        Assert.IsTrue(store.TryMarkSubscribeSending(token, out var first));
        store.AddOrUpdate(token, 1, new EventFilter("/locks/a", "changed", 1));
        Assert.IsTrue(store.TryMarkSubscribeSending(token, out var replacement));
        Assert.IsFalse(store.IsCurrent(first!));
        store.MarkSubscribeSent(first!, DateTime.UtcNow);
        store.MarkSubscribeSendFailed(first!, "late failure");
        Assert.IsTrue(replacement!.SubscribeSendInProgress);
        Assert.IsNull(replacement.SubscribeSentAtUtc);
        Assert.IsNull(replacement.LastError);
        store.ResetForReconnect();
        Assert.IsTrue(store.TryMarkSubscribeSending(token, out var reconnected));
        store.MarkSubscribeSent(replacement, DateTime.UtcNow);
        store.MarkSubscribeSendFailed(replacement, "old socket failure");
        Assert.IsFalse(store.IsCurrent(replacement));
        Assert.IsTrue(reconnected!.SubscribeSendInProgress);
        Assert.IsNull(reconnected.SubscribeSentAtUtc);
        Assert.IsNull(reconnected.LastError);
        store.Remove(token);
        Assert.IsFalse(store.IsCurrent(reconnected));
    }

    [TestMethod]
    public async Task SessionIndex_CleansActiveAndRejectedRequestsWithoutRemovingOtherSessions()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var a = new WebSocketSession(new(), manager, new SinkSocket());
        using var b = new WebSocketSession(new(), manager, new SinkSocket());
        Assert.IsTrue((await manager.SubscribeAsync(a, Guid.NewGuid(), 0, new EventFilter("/locks/a", "changed"))).Accepted);
        var bToken = Guid.NewGuid();
        var keep = manager.SubscribeAsync(b, bToken, 0, new EventFilter("/locks/a", "changed"));
        Assert.IsTrue((await keep).Accepted);
        var invalid = new EventFilter();
        invalid.SourcesAndTypes.Add(new() { Source = "[", EventType = "changed" });
        Assert.IsFalse((await manager.SubscribeAsync(a, Guid.NewGuid(), 0, invalid)).Accepted);
        manager.UnsubscribeSession(a.Id);
        Assert.AreEqual(1, manager.RemoteSubscriptionCount);
        Assert.IsTrue(manager.IsCurrent(b.Id, bToken, keep));
        Assert.IsFalse(Field<Dictionary<Guid, HashSet<RemoteSubscriptionKey>>>(manager, "sessionRequests").ContainsKey(a.Id));
        manager.UnsubscribeSession(b.Id);
        Assert.AreEqual(0, manager.AggregatedSubscriptionCount);
        Assert.IsEmpty(Field<Dictionary<Guid, HashSet<RemoteSubscriptionKey>>>(manager, "sessionRequests"));
    }

    private static T Field<T>(object target, string name) => (T)target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(target)!;
    private static async Task Until(Func<bool> predicate)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(4));
        while (!predicate()) await Task.Delay(5, timeout.Token);
    }
    private sealed class NeverDelay : IWebSocketDelayProvider
    {
        public Task DelayAsync(TimeSpan delay, CancellationToken token) => Task.Delay(Timeout.InfiniteTimeSpan, token);
    }
    private sealed class BlockingCleanup(IDisposable inner) : IDisposable
    {
        internal readonly ManualResetEventSlim Entered = new(), Release = new();
        public void Dispose() { Entered.Set(); Release.Wait(TimeSpan.FromSeconds(5)); inner.Dispose(); }
    }
    private sealed class BlockingReuseLogger : ICfet2Logger, IDisposable
    {
        internal readonly ManualResetEventSlim Entered = new(), Release = new();
        public void Error(string message)
        {
            if (!message.Contains("existing failed subscription reused")) return;
            Entered.Set(); Release.Wait(TimeSpan.FromSeconds(5));
        }
        public void Trace(string message) { }
        public void Debug(string message) { }
        public void Info(string message) { }
        public void Warn(string message) { }
        public void Fatal(string message) { }
        public void Dispose() { Release.Set(); }
    }
    private sealed class SinkSocket : WebSocket
    {
        public override WebSocketCloseStatus? CloseStatus => null;
        public override string? CloseStatusDescription => null;
        public override WebSocketState State => WebSocketState.Open;
        public override string? SubProtocol => null;
        public override void Abort() { }
        public override void Dispose() { }
        public override Task CloseAsync(WebSocketCloseStatus status, string? description, CancellationToken token) => Task.CompletedTask;
        public override Task CloseOutputAsync(WebSocketCloseStatus status, string? description, CancellationToken token) => Task.CompletedTask;
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken token) => throw new NotSupportedException();
        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType type, bool end, CancellationToken token) => Task.CompletedTask;
    }
}
