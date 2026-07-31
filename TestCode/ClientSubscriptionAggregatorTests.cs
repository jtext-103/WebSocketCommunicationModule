using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed class ClientSubscriptionAggregatorTests : CFET2Host
{
    private const string Endpoint = "ws://127.0.0.1:12345/cfet2/event/v2";

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
    public void Subscribe_SingleRule_ShouldCreateOneSubscribeAction()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var token = CreateToken();

        var plan = aggregator.Subscribe(token, CreateFilter("/thing/a"));

        Assert.HasCount(1, plan.Actions);
        Assert.AreEqual(RemoteEndpointActionKind.Subscribe, plan.Actions[0].Kind);
    }

    [TestMethod]
    public void Subscribe_MultipleRulesWithDuplicate_ShouldCreateDistinctSubscribeActions()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var filter = CreateFilter("/thing/a");
        filter.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/b", EventType = "Changed" });
        filter.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/a", EventType = "Changed" });

        var plan = aggregator.Subscribe(CreateToken(), filter);
        var subscribeActions = SubscribeActions(plan);

        Assert.HasCount(2, subscribeActions);
        CollectionAssert.AreEquivalent(
            new[] { "/thing/a", "/thing/b" },
            subscribeActions.Select(action => action.RemoteFilter!.SourcesAndTypes.Single().Source).ToArray());
    }

    [TestMethod]
    public void Subscribe_SharedRule_ShouldSubscribeOnceAndUnsubscribeOnlyAfterLastLocalToken()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var token1 = CreateToken();
        var token2 = CreateToken();

        var firstPlan = aggregator.Subscribe(token1, CreateFilter("/thing/a"));
        var secondPlan = aggregator.Subscribe(token2, CreateFilter("/thing/a"));
        var unsubscribeFirst = aggregator.Unsubscribe(token1);
        var unsubscribeLast = aggregator.Unsubscribe(token2);

        Assert.HasCount(1, firstPlan.Actions);
        Assert.HasCount(1, unsubscribeLast.Actions);
        Assert.IsFalse(aggregator.HasLocalSubscriptions(RemoteEndpointKey.Parse(Endpoint)));
    }

    [TestMethod]
    public void Subscribe_CaseVariantRules_ShouldAggregateIntoOneRemoteSubscription()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var firstPlan = aggregator.Subscribe(CreateToken(), CreateFilter("/Thing/Status"));

        var secondPlan = aggregator.Subscribe(CreateToken(), CreateFilter("/thing/status"));

        Assert.HasCount(1, firstPlan.Actions);
        Assert.IsTrue(secondPlan.IsEmpty);
    }

    [TestMethod]
    public void Subscribe_SameTokenSameGroupSet_ShouldBeIdempotent()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var token = CreateToken();
        var filterAB = CreateFilter("/thing/a");
        filterAB.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/b", EventType = "Changed" });
        var filterBA = CreateFilter("/thing/b");
        filterBA.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/a", EventType = "Changed" });

        var firstPlan = aggregator.Subscribe(token, filterAB);
        var secondPlan = aggregator.Subscribe(token, filterBA);

        Assert.HasCount(2, firstPlan.Actions);
        Assert.IsTrue(secondPlan.IsEmpty);
    }

    [TestMethod]
    public void Subscribe_MixedPerformanceLevels_ShouldUpdateOneRemoteSubscription()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var level1Token = CreateToken();
        var level0Token = CreateToken();

        var firstPlan = aggregator.Subscribe(level1Token, CreateFilter("/thing/a", 1));
        var updatePlan = aggregator.Subscribe(level0Token, CreateFilter("/thing/a", 0));

        Assert.HasCount(1, firstPlan.Actions);
        Assert.HasCount(1, updatePlan.Actions);
        Assert.AreEqual(firstPlan.Actions[0].MasterTokenId, updatePlan.Actions[0].MasterTokenId);
        Assert.AreEqual(0, firstPlan.Actions[0].Revision);
        Assert.AreEqual(1, updatePlan.Actions[0].Revision);
        Assert.AreEqual(1, firstPlan.Actions[0].RemoteFilter!.PerformanceLevel);
        Assert.AreEqual(0, updatePlan.Actions[0].RemoteFilter!.PerformanceLevel);
    }

    [TestMethod]
    public void Unsubscribe_LastLevel0_ShouldUpdateSharedRemoteSubscriptionToLevel1()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var level1Token = CreateToken();
        var level0Token = CreateToken();
        aggregator.Subscribe(level1Token, CreateFilter("/thing/a", 1));
        var level0Plan = aggregator.Subscribe(level0Token, CreateFilter("/thing/a", 0));

        var updatePlan = aggregator.Unsubscribe(level0Token);

        Assert.HasCount(1, updatePlan.Actions);
        Assert.AreEqual(RemoteEndpointActionKind.Subscribe, updatePlan.Actions[0].Kind);
        Assert.AreEqual(level0Plan.Actions[0].MasterTokenId, updatePlan.Actions[0].MasterTokenId);
        Assert.AreEqual(2, updatePlan.Actions[0].Revision);
        Assert.AreEqual(1, updatePlan.Actions[0].RemoteFilter!.PerformanceLevel);
    }

    [TestMethod]
    public void Subscribe_SameTokenPerformanceLevelChange_ShouldNotBeIdempotent()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var token = CreateToken();
        var firstPlan = aggregator.Subscribe(token, CreateFilter("/thing/a", 1));

        var updatePlan = aggregator.Subscribe(token, CreateFilter("/thing/a", 0));

        Assert.HasCount(1, updatePlan.Actions);
        Assert.AreEqual(firstPlan.Actions[0].MasterTokenId, updatePlan.Actions[0].MasterTokenId);
        Assert.AreEqual(1, updatePlan.Actions[0].Revision);
        Assert.AreEqual(0, updatePlan.Actions[0].RemoteFilter!.PerformanceLevel);
    }

    [TestMethod]
    public async Task ConcurrentLevelUpdatesAndCancellation_ShouldProduceOneUpdatePerEffectiveLevelChange()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var level1Token = CreateToken();
        var initialPlan = aggregator.Subscribe(level1Token, CreateFilter("/thing/a", 1));
        var level0Tokens = Enumerable.Range(0, 32).Select(_ => CreateToken()).ToArray();

        var subscribePlans = await Task.WhenAll(level0Tokens.Select(token =>
            Task.Run(() => aggregator.Subscribe(token, CreateFilter("/thing/a", 0)))));
        var subscribeUpdates = subscribePlans.SelectMany(plan => plan.Actions).ToArray();

        var unsubscribePlans = await Task.WhenAll(level0Tokens.Select(token =>
            Task.Run(() => aggregator.Unsubscribe(token))));
        var unsubscribeUpdates = unsubscribePlans.SelectMany(plan => plan.Actions).ToArray();

        Assert.HasCount(1, subscribeUpdates);
        Assert.HasCount(1, unsubscribeUpdates);
        Assert.AreEqual(initialPlan.Actions[0].MasterTokenId, subscribeUpdates[0].MasterTokenId);
        Assert.AreEqual(initialPlan.Actions[0].MasterTokenId, unsubscribeUpdates[0].MasterTokenId);
        Assert.AreEqual(1, subscribeUpdates[0].Revision);
        Assert.AreEqual(0, subscribeUpdates[0].RemoteFilter!.PerformanceLevel);
        Assert.AreEqual(2, unsubscribeUpdates[0].Revision);
        Assert.AreEqual(1, unsubscribeUpdates[0].RemoteFilter!.PerformanceLevel);
    }

    [TestMethod]
    public void Subscribe_SameTokenChangedGroupSet_ShouldSubscribeNewAndUnsubscribeStale()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var token = CreateToken();
        var filterAB = CreateFilter("/thing/a");
        filterAB.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/b", EventType = "Changed" });
        var filterBC = CreateFilter("/thing/b");
        filterBC.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/c", EventType = "Changed" });

        var firstPlan = aggregator.Subscribe(token, filterAB);
        var updatePlan = aggregator.Subscribe(token, filterBC);
        var firstSubscribeActions = SubscribeActions(firstPlan);
        var updateSubscribeActions = SubscribeActions(updatePlan);
        var updateUnsubscribeActions = UnsubscribeActions(updatePlan);

        Assert.HasCount(2, firstSubscribeActions);
        Assert.HasCount(1, updateSubscribeActions);
        Assert.HasCount(1, updateUnsubscribeActions);
        Assert.HasCount(2, updatePlan.Actions);
        Assert.AreEqual(RemoteEndpointActionKind.Unsubscribe, updatePlan.Actions[0].Kind);
        Assert.AreEqual(RemoteEndpointActionKind.Subscribe, updatePlan.Actions[1].Kind);
        Assert.AreEqual("/thing/c", updateSubscribeActions[0].RemoteFilter!.SourcesAndTypes.Single().Source);
        Assert.AreEqual(
            firstSubscribeActions.Single(action => action.RemoteFilter!.SourcesAndTypes.Single().Source == "/thing/a").MasterTokenId,
            updateUnsubscribeActions[0].MasterTokenId);
    }

    [TestMethod]
    public void Clear_ShouldRemoveAllLocalSubscriptionState()
    {
        var aggregator = new ClientSubscriptionAggregator();
        var endpoint = RemoteEndpointKey.Parse(Endpoint);
        aggregator.Subscribe(CreateToken(), CreateFilter("/thing/a"));

        aggregator.Clear();

        Assert.IsFalse(aggregator.HasLocalSubscriptions(endpoint));
    }

    private Token CreateToken()
    {
        return new Token(MyHub.EventHub, "ws", "ws://127.0.0.1:1/cfet2/event/v2");
    }

    private static EventFilter CreateFilter(string source, int performanceLevel = 0)
    {
        return new EventFilter(source, "Changed", performanceLevel, Endpoint);
    }

    private static ClientRemoteAction[] SubscribeActions(ClientSubscriptionPlan plan)
    {
        return plan.Actions
            .Where(action => action.Kind == RemoteEndpointActionKind.Subscribe)
            .ToArray();
    }

    private static ClientRemoteAction[] UnsubscribeActions(ClientSubscriptionPlan plan)
    {
        return plan.Actions
            .Where(action => action.Kind == RemoteEndpointActionKind.Unsubscribe)
            .ToArray();
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
}
