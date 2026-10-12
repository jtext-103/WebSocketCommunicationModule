using System.Collections.Concurrent;
using System.Reflection;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions;

namespace TestCode;

public sealed partial class WebSocketRegexSubscriptionLifecycleTests
{
    private static AggregatedServerSubscription[] Groups(WebSocketSubscriptionManager manager) =>
        ((ConcurrentDictionary<ServerSubscriptionKey, AggregatedServerSubscription>)typeof(WebSocketSubscriptionManager)
            .GetField("aggregatedSubscriptions", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!).Values.ToArray();

    private sealed class CleanupLog : ICfet2Logger, IDisposable
    {
        private readonly ICfet2Logger previous = WebSocketLog.Logger;
        public readonly ConcurrentQueue<string> Errors = new();
        public CleanupLog() => WebSocketLog.Initialize(this);
        public void Dispose() => WebSocketLog.Initialize(previous);
        public void Trace(string message) { }
        public void Debug(string message) { }
        public void Info(string message) { }
        public void Warn(string message) { }
        public void Error(string message) => Errors.Enqueue(message);
        public void Fatal(string message) { }
    }

    [TestMethod]
    public async Task CancellingPartiallyPreparedBatchRetainsHandlesOnlyUntilBlockedGetterReturns()
    {
        Add("ready"); var slow = Add(block: true);
        using var manager = new WebSocketSubscriptionManager(MyHub, 1); using var session = Session(manager);
        var id = Guid.NewGuid();
        var task = Subscribe(manager, session, id, "/(ready/Value|device/Value)");
        try
        {
            Assert.IsTrue(slow.Entered.Wait(2000)); Assert.AreEqual(1, Observed);
            manager.Unsubscribe(session.Id, id);
            Assert.IsTrue(task.IsCanceled); Assert.AreEqual(1, Observed, "The incomplete Core batch cannot be disposed until returned.");
            Assert.AreEqual(1, manager.RunningPreparationCount);
            slow.Release.Set(); await Eventually(() => manager.RunningPreparationCount == 0);
            Assert.AreEqual(0, Observed); Assert.AreEqual(0, Events); Assert.AreEqual(0, manager.PendingRequestCount);
        }
        finally { slow.Release.Set(); }
    }

    // C09: same session/token lifecycle, with the old getter still running at the new commit.
    [TestMethod]
    public async Task NewRevisionCommitsBeforeOldGetterReturnsAndSurvivesLateCleanup()
    {
        var slow = Add(block: true); Add("fast");
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var session = Session(manager); var id = Guid.NewGuid();
        var old = Subscribe(manager, session, id);
        try
        {
            Assert.IsTrue(slow.Entered.Wait(2000));
            var current = Subscribe(manager, session, id, "/fast/.*", 1);
            await Accepted(current);
            Assert.IsTrue(old.IsCanceled); Assert.IsFalse(slow.Release.IsSet);
            var committed = Groups(manager).Single();
            slow.Release.Set(); await Eventually(() => manager.RunningPreparationCount == 0);
            Assert.AreSame(committed, Groups(manager).Single());
            Assert.AreSame(current, Subscribe(manager, session, id, "/fast/.*", 1));
            Assert.AreEqual(2, Observed); Assert.AreEqual(1, Events);
            manager.Unsubscribe(session.Id, id);
            Assert.AreEqual(0, Observed); Assert.AreEqual(0, Events);
        }
        finally { slow.Release.Set(); }
    }

    // R09: a resource-level failure must not turn an update into a rejected WS request.
    [TestMethod]
    public async Task RevisionWithPartiallyUnreadableResourcesCommitsAndDoesNotRetry()
    {
        Add("old"); var partial = Add(); partial.Fail = true;
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var session = Session(manager); var id = Guid.NewGuid();
        await Accepted(Subscribe(manager, session, id, "/old/.*"));
        await Accepted(Subscribe(manager, session, id, revision: 1));
        var group = (ResourceGroupSubscription)Groups(manager).Single().LocalSubscription!;
        Assert.AreEqual(ResourceObservationStatus.Failed, group.ObservationResults.Single(r => r.ResourcePath.EndsWith("/Value")).Status);
        Assert.AreEqual(ResourceObservationStatus.Started, group.ObservationResults.Single(r => r.ResourcePath.EndsWith("/Pressure")).Status);
        Assert.AreEqual(1, Observed); Assert.AreEqual(1, Events);
        partial.Fail = false;
        await Accepted(Subscribe(manager, session, id, revision: 2, level: 1));
        Assert.AreEqual(1, partial.Reads);
        manager.Unsubscribe(session.Id, id); Assert.AreEqual(0, Observed);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task FailedRevisionCacheIsRemovedAtSubscriptionOrSessionEnd(bool closeSession)
    {
        var device = Add(); device.Fail = true;
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var session = Session(manager); var id = Guid.NewGuid();
        var failed = Subscribe(manager, session, id, "/device/Value");
        Assert.IsFalse((await failed).Accepted);
        device.Fail = false;
        Assert.AreSame(failed, Subscribe(manager, session, id, "/device/Value"));
        if (closeSession) manager.UnsubscribeSession(session.Id); else manager.Unsubscribe(session.Id, id);
        var retry = Subscribe(manager, session, id, "/device/Value");
        Assert.AreNotSame(failed, retry); await Accepted(retry);
        Assert.AreEqual(2, device.Reads);
    }

    [TestMethod]
    public async Task EquivalentRegexTextKeepsSeparateGroupsButSharesObservation()
    {
        var device = Add(); using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var session = Session(manager); var a = Guid.NewGuid(); var b = Guid.NewGuid();
        await Accepted(Subscribe(manager, session, a, "/device/(Value)"));
        await Accepted(Subscribe(manager, session, b, "/device/(?:Value)"));
        Assert.AreEqual(2, manager.AggregatedSubscriptionCount); Assert.AreEqual(1, Observed);
        Assert.AreEqual(1, device.Reads);
        manager.Unsubscribe(session.Id, a); Assert.AreEqual(1, Observed);
        manager.Unsubscribe(session.Id, b); Assert.AreEqual(0, Observed);
    }

    // A registered no-argument exact path uses SubscribeResource's literal source filter.
    [TestMethod]
    public async Task RegisteredMetacharacterPathUsesExactObservationAndLiteralSourceMatching()
    {
        var dotted = Add("a.b"); var wildcard = Add("axb");
        using var manager = new WebSocketSubscriptionManager(MyHub, 1); using var socket = new SinkSocket();
        using var session = new Jtext103.CFET2.WebSocketCommunicationModule.Server.WebSocketSession(
            new Jtext103.CFET2.WebSocketCommunicationModule.Options.WebSocketCommunicationOptions(), manager, socket);
        await Accepted(Subscribe(manager, session, Guid.NewGuid(), "/a.b/Value"));
        Assert.IsInstanceOfType<ResourceSubscription>(Groups(manager).Single().LocalSubscription);
        Assert.AreEqual(1, Observed); Assert.AreEqual(1, dotted.Reads); Assert.AreEqual(0, wildcard.Reads);
        MyHub.Publish(new EventArg("/axb/Value", "changed", new Status<int>(1)));
        MyHub.Publish(new EventArg("/a.b/Value", "changed", new Status<int>(1)));
        await Eventually(() => socket.SendCount == 1);
        await Task.Delay(50);
        Assert.AreEqual(1, socket.SendCount, "Exact subscription must not treat a literal dot as a wildcard.");
        manager.UnsubscribeSession(session.Id);
        await Accepted(Subscribe(manager, session, Guid.NewGuid(), @"/a\.b/Value"));
        Assert.AreEqual(1, Observed); Assert.AreEqual(2, dotted.Reads); Assert.AreEqual(0, wildcard.Reads);
        MyHub.Publish(new EventArg("/axb/Value", "changed", new Status<int>(2)));
        MyHub.Publish(new EventArg("/a.b/Value", "changed", new Status<int>(3)));
        await Eventually(() => socket.SendCount >= 2 && manager.ActiveFanOutEventCount == 0);
        Assert.AreEqual(2, socket.SendCount, "Escaping the dot restricts active-event matching too.");
    }

    [TestMethod]
    [DataRow(@"/x/\d", @"\w", "/x/1", "a", "/x/a", "!")]
    [DataRow(@"/x/\D", @"\W", "/x/a", "!", "/x/1", "a")]
    public async Task RegexEscapeCasesActuallyFilterEvents(string source, string type, string goodSource, string goodType, string badSource, string badType)
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1); using var socket = new SinkSocket();
        using var session = new Jtext103.CFET2.WebSocketCommunicationModule.Server.WebSocketSession(
            new Jtext103.CFET2.WebSocketCommunicationModule.Options.WebSocketCommunicationOptions(), manager, socket);
        await Accepted(Subscribe(manager, session, Guid.NewGuid(), source, type: type));
        MyHub.Publish(new EventArg(badSource, goodType, new Status<int>(1)));
        MyHub.Publish(new EventArg(goodSource, badType, new Status<int>(2)));
        MyHub.Publish(new EventArg(goodSource, goodType, new Status<int>(3)));
        await Eventually(() => socket.SendCount > 0 && manager.ActiveFanOutEventCount == 0);
        Assert.AreEqual(1, socket.SendCount);
    }
}
