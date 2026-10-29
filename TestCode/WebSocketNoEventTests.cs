using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Attributes;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

public sealed partial class WebSocketServerPhase7Tests
{
    private sealed class NoEventDevice : Thing
    {
        public int Reads;
        [Cfet2Status, Cfet2NoEvent] public int Quiet { get { Reads++; return 0; } }
        [Cfet2Status] public int Normal => 1;
    }

    [TestMethod, DataRow(false), DataRow(true)]
    public async Task NoEventResourceAckSkipsWithoutActiveSubscriptionsAndIsIdempotent(bool regex)
    {
        var device = new NoEventDevice(); MyHub.TryAddThing(device, "/", "noevent");
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        using var client = await ConnectAndHelloAsync(listen);
        var request = new SubscribeMessage { TokenId = Guid.NewGuid(), Filter = new EventFilter(regex ? "/noevent/Qui.*" : "/noevent/Quiet", "changed") { IsResourceSubscription = true } };
        for (int i = 0; i < 2; i++)
        {
            await SendAsync(client, request);
            var ack = await ReceiveUntilAsync<SubscribeAckMessage>(client);
            Assert.IsTrue(ack.Accepted); Assert.IsTrue(ack.Skipped);
            CollectionAssert.Contains(ack.SkippedResources!, "/noevent/Quiet");
        }
        Assert.AreEqual(0, device.Reads);
        Assert.AreEqual(0, GetObservedResourceCount());
        Assert.AreEqual(0, GetLocalSubscriptionCount());
        Assert.AreEqual(0, GetRemoteSubscriptionCount(module));
        Assert.AreEqual(0, module.GetStatus().ServerAggregatedSubscriptionCount);
        await SendAsync(client, new UnsubscribeMessage { TokenId = request.TokenId });
    }

    [TestMethod, DataRow(0), DataRow(1)]
    public async Task NoEventMixedResourceAndOrdinarySubscriptionsStaySeparate(int level)
    {
        var device = new NoEventDevice(); MyHub.TryAddThing(device, "/", "noevent");
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        using var client = await ConnectAndHelloAsync(listen);
        var resourceId = Guid.NewGuid(); var ordinaryId = Guid.NewGuid();
        await SendAsync(client, new SubscribeMessage { TokenId = resourceId, Filter = new EventFilter("/noevent/.*", "alarm") { IsResourceSubscription = true, PerformanceLevel = level } });
        var ack = await ReceiveUntilAsync<SubscribeAckMessage>(client);
        Assert.IsTrue(ack.Accepted); Assert.IsFalse(ack.Skipped);
        CollectionAssert.Contains(ack.SkippedResources!, "/noevent/Quiet");
        await SendAsync(client, new SubscribeMessage { TokenId = ordinaryId, Filter = new EventFilter("/noevent/.*", "alarm") { PerformanceLevel = level } });
        Assert.IsTrue((await ReceiveUntilAsync<SubscribeAckMessage>(client)).Accepted);
        Assert.AreEqual(2, module.GetStatus().ServerAggregatedSubscriptionCount);
        MyHub.Publish("/noevent/Quiet", "alarm", 5);
        var quiet = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(ordinaryId, quiet.TokenId); Assert.IsTrue(quiet.EventArg!.NoEvent);
        MyHub.Publish("/noevent/Normal", "alarm", 6);
        var first = await ReceiveUntilAsync<RemoteEventMessage>(client);
        var second = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual("/noevent/Normal", first.EventArg!.Source);
        Assert.AreEqual("/noevent/Normal", second.EventArg!.Source);
        CollectionAssert.AreEquivalent(new[] { resourceId, ordinaryId }, new[] { first.TokenId!.Value, second.TokenId!.Value });
        Assert.AreEqual(0, device.Reads); Assert.AreEqual(0, GetObservedResourceCount());
        client.Abort();
        Assert.IsTrue(await WaitUntilAsync(() => GetLocalSubscriptionCount() == 0 && GetRemoteSubscriptionCount(module) == 0));
    }

    [TestMethod]
    public async Task NoEventMixedBatchKeepsActiveRulesAndReleasesObservationsAcrossSessions()
    {
        var device = new NoEventDevice(); MyHub.TryAddThing(device, "/", "noevent");
        var listen = CreateListenUri(); using var module = StartServerModule(listen);
        var filter = new EventFilter("/noevent/Quiet", "changed") { IsResourceSubscription = true };
        filter.AddSourceAndType("/noevent/Normal", "changed");
        filter.AddSourceAndType("/unregistered", "alarm");
        var tokenId = Guid.NewGuid();
        for (var attempt = 0; attempt < 2; attempt++)
        {
            using var client = await ConnectAndHelloAsync(listen);
            await SendAsync(client, new SubscribeMessage { TokenId = tokenId, Filter = filter });
            var ack = await ReceiveUntilAsync<SubscribeAckMessage>(client);
            Assert.IsTrue(ack.Accepted); Assert.IsFalse(ack.Skipped);
            CollectionAssert.AreEqual(new[] { "/noevent/Quiet" }, ack.SkippedResources!);
            Assert.AreEqual(0, device.Reads);
            Assert.AreEqual(1, GetObservedResourceCount());
            Assert.AreEqual(2, GetLocalSubscriptionCount());
            Assert.AreEqual(1, GetRemoteSubscriptionCount(module));
            MyHub.Publish("/unregistered", "alarm", 7);
            var received = await ReceiveUntilAsync<RemoteEventMessage>(client);
            Assert.AreEqual(tokenId, received.TokenId);
            Assert.AreEqual("/unregistered", received.EventArg!.Source);
            if (attempt == 0) client.Abort();
            else await SendAsync(client, new UnsubscribeMessage { TokenId = tokenId });
            Assert.IsTrue(await WaitUntilAsync(() => GetObservedResourceCount() == 0 &&
                GetLocalSubscriptionCount() == 0 && GetRemoteSubscriptionCount(module) == 0));
        }
    }

    [TestMethod]
    public void NoEventClientAggregationSeparatesKindsAndPreservesSkipDiagnostics()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var a = new Token(MyHub.EventHub); var b = new Token(MyHub.EventHub);
        var filter = new EventFilter("/noevent/Quiet", "changed") { Host = "ws://localhost:12345" };
        aggregator.Subscribe(a, filter);
        var plan = aggregator.Subscribe(b, new EventFilter(filter) { IsResourceSubscription = true });
        Assert.AreEqual(2, aggregator.GroupCount);
        Assert.IsTrue(plan.Actions.Single().RemoteFilter!.IsResourceSubscription);
        var store = new RemoteEndpointSubscriptionStore(RemoteEndpointKey.Parse(filter.Host));
        store.AddOrUpdate(b.Id, 0, filter);
        Assert.IsNull(store.MarkSubscribeAck(new SubscribeAckMessage { TokenId = b.Id, Accepted = true, Skipped = true, SkippedResources = new[] { "/noevent/Quiet" } }));
        Assert.AreEqual(SubscriptionState.Skipped, store.Get(b.Id)!.State);
        Assert.IsNull(store.Get(b.Id)!.LastError);
        Assert.AreEqual(0, store.GetSubscribeAckTimeouts(DateTime.UtcNow.AddHours(1), TimeSpan.FromSeconds(1)).Count);
        store.ResetForReconnect();
        Assert.AreEqual(SubscriptionState.Pending, store.Get(b.Id)!.State);
        aggregator.Unsubscribe(a); aggregator.Unsubscribe(b);
        Assert.AreEqual(0, aggregator.GroupCount);
    }
}
