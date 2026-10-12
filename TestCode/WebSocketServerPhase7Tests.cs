using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Attributes;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed partial class WebSocketServerPhase7Tests : CFET2Host
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
    public async Task Server_ShouldAcceptHelloAndSubscribe()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();

        await SendAsync(client, new SubscribeMessage
        {
            TokenId = tokenId,
            Filter = new EventFilter("/thing/status", "Changed"),
        });

        var ack = (SubscribeAckMessage)await ReceiveAsync(client);
        Assert.AreEqual(tokenId, ack.TokenId);
        Assert.IsTrue(ack.Accepted);
    }

    [TestMethod]
    public async Task Diagnostics_ShouldReportServerSessionAndSubscriptions()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();

        await SubscribeAsync(client, tokenId, "/thing/status", "Changed");

        Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().ServerRemoteSubscriptionCount == 1));
        var status = module.GetStatus();
        var session = module.GetServerSessions().Single();

        Assert.IsTrue(status.ServerStarted);
        Assert.AreEqual(1, status.ServerActiveSessionCount);
        Assert.AreEqual(1, status.ServerRemoteSubscriptionCount);
        Assert.AreEqual(1, status.ServerAggregatedSubscriptionCount);
        Assert.IsNotNull(session.HandshakeCompletedAtUtc);
        Assert.AreEqual(1, session.RemoteSubscriptionCount);
        Assert.IsTrue(module.PrintStatus().Contains("serverSessions=1", StringComparison.Ordinal));
        Assert.IsTrue(module.PrintServerSessions().Contains(session.SessionId.ToString(), StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task Server_ShouldPushLocalEventToRemoteSubscriber()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();
        await SubscribeAsync(client, tokenId, "/thing/status", "Changed");

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(42)));
#pragma warning restore CS0612

        var remoteEvent = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.IsNotNull(remoteEvent.EventArg);
        Assert.AreEqual("/thing/status", remoteEvent.EventArg.Source);
        Assert.AreEqual("Changed", remoteEvent.EventArg.EventType);
        Assert.AreEqual(42, remoteEvent.EventArg.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_ShouldTransformRemoteFilterToLocalFilter()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        await SubscribeAsync(
            client,
            Guid.NewGuid(),
            "/thing/status",
            "Changed",
            "ws://should-not-be-used:9999/cfet2/event/v2");

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<string>("local")));
#pragma warning restore CS0612

        var remoteEvent = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual("local", remoteEvent.EventArg!.Sample.GetVal<string>());
    }

    [TestMethod]
    public async Task Server_Unsubscribe_ShouldStopPushForThatToken()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();
        await SubscribeAsync(client, tokenId, "/thing/status", "Changed");

        await SendAsync(client, new UnsubscribeMessage { TokenId = tokenId });
        await Task.Delay(100);

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(7)));
#pragma warning restore CS0612

        Assert.IsFalse(await TryReceiveEventAsync(client, TimeSpan.FromMilliseconds(300)));
    }

    [TestMethod]
    public async Task Server_SessionClose_ShouldCleanupLocalSubscription()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using (var client = await ConnectAndHelloAsync(listenUri))
        {
            await SubscribeAsync(client, Guid.NewGuid(), "/thing/status", "Changed");
        }

        await Task.Delay(200);

        using var secondClient = await ConnectAndHelloAsync(listenUri);
        await SubscribeAsync(secondClient, Guid.NewGuid(), "/thing/status", "Changed");

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(99)));
#pragma warning restore CS0612

        var remoteEvent = await ReceiveUntilAsync<RemoteEventMessage>(secondClient);
        Assert.AreEqual(99, remoteEvent.EventArg!.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_ShouldReplyPongToPing()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);

        await SendAsync(client, new PingMessage());

        var pong = await ReceiveUntilAsync<PongMessage>(client);
        Assert.AreEqual(WebSocketProtocolConstants.Pong, pong.MessageType);
    }

    [TestMethod]
    public async Task Server_IdleTimeout_ShouldCloseInactiveSession()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(
            listenUri,
            heartbeatInterval: TimeSpan.FromMilliseconds(30),
            serverIdleTimeout: TimeSpan.FromMilliseconds(100));
        using var client = await ConnectAndHelloAsync(listenUri);

        Assert.IsTrue(await WaitUntilAsync(() => GetServerSessionCount(module) == 0));
        await AssertThrowsAsync<WebSocketException>(() => ReceiveAsync(client, 1000));
    }

    [TestMethod]
    public async Task Server_MaxSessions_ShouldRejectAdditionalUpgrade()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri, maxServerSessions: 1);
        using var firstClient = await ConnectAndHelloAsync(listenUri);
        using var secondClient = new ClientWebSocket();
        secondClient.Options.AddSubProtocol(WebSocketProtocolConstants.SubProtocol);

        await AssertThrowsAsync<WebSocketException>(() =>
            secondClient.ConnectAsync(ToWebSocketUri(listenUri), CancellationToken.None));
        Assert.AreEqual(1, GetServerSessionCount(module));
    }

    [TestMethod]
    public async Task Server_HttpRequestToEndpoint_ShouldReturnBadRequest()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var httpClient = new HttpClient();

        var response = await httpClient.GetAsync(ToHttpUri(listenUri));

        Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [TestMethod]
    public async Task Server_HttpRequestToWrongPath_ShouldReturnNotFound()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var httpClient = new HttpClient();

        var wrongPath = new Uri($"http://{listenUri.Host}:{listenUri.Port}/wrong/path");
        var response = await httpClient.GetAsync(wrongPath);

        Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
    }

    [TestMethod]
    public async Task Server_WebSocketWithoutSubProtocol_ShouldRejectUpgrade()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = new ClientWebSocket();

        await AssertThrowsAsync<WebSocketException>(() =>
            client.ConnectAsync(ToWebSocketUri(listenUri), CancellationToken.None));
    }

    [TestMethod]
    public async Task Server_FirstMessageNotHello_ShouldSendErrorAndCloseSession()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAsync(listenUri);

        await SendAsync(client, new PingMessage());

        var error = (ErrorMessage)await ReceiveAsync(client);
        Assert.AreEqual("handshakeExpected", error.Code);
        await AssertThrowsAsync<WebSocketException>(() => ReceiveAsync(client, 1000));
    }

    [TestMethod]
    public async Task Server_OversizedMessage_ShouldCloseSession()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri, maxMessageBytes: 64);
        using var client = await ConnectAsync(listenUri);

        await SendRawTextAsync(client, new string('x', 128));

        await AssertThrowsAsync<WebSocketException>(() => ReceiveAsync(client, 1000));
    }

    [TestMethod]
    public async Task Server_InvalidJsonAfterHandshake_ShouldCloseOnlyThatSession()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var invalidClient = await ConnectAndHelloAsync(listenUri);
        using var healthyClient = await ConnectAndHelloAsync(listenUri);

        await SendRawTextAsync(invalidClient, "{ invalid json");

        await AssertThrowsAsync<WebSocketException>(() => ReceiveAsync(invalidClient, 1000));
        await SubscribeAsync(healthyClient, Guid.NewGuid(), "/thing/status", "Changed");
    }

    [TestMethod]
    public async Task Server_WrongProtocolAfterHandshake_ShouldCloseOnlyThatSession()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var invalidClient = await ConnectAndHelloAsync(listenUri);
        using var healthyClient = await ConnectAndHelloAsync(listenUri);

        await SendRawTextAsync(invalidClient, """
            {
              "messageType": "ping",
              "protocol": "wrong.protocol",
              "version": 2
            }
            """);

        await AssertThrowsAsync<WebSocketException>(() => ReceiveAsync(invalidClient, 1000));
        await SendAsync(healthyClient, new PingMessage());
        _ = await ReceiveUntilAsync<PongMessage>(healthyClient);
    }

    [TestMethod]
    public async Task Server_AbortClient_ShouldCleanupSessionAndRemainUsable()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        var abortedClient = await ConnectAndHelloAsync(listenUri);
        await SubscribeAsync(abortedClient, Guid.NewGuid(), "/thing/aborted", "Changed");

        abortedClient.Abort();
        abortedClient.Dispose();

        Assert.IsTrue(await WaitUntilAsync(() => GetServerSessionCount(module) == 0));
        using var healthyClient = await ConnectAndHelloAsync(listenUri);
        await SubscribeAsync(healthyClient, Guid.NewGuid(), "/thing/healthy", "Changed");
    }

    [TestMethod]
    public async Task Server_EventSendFailureAfterClientAbort_ShouldCleanupSession()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        var client = await ConnectAndHelloAsync(listenUri);
        await SubscribeAsync(client, Guid.NewGuid(), "/thing/status", "Changed");

        client.Abort();
        client.Dispose();
#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(42)));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() =>
            GetServerSessionCount(module) == 0 &&
            GetLocalSubscriptionCount() == 0));
    }

    [TestMethod]
    public async Task Server_ShouldServeConcurrentSubscribersIndependently()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client1 = await ConnectAndHelloAsync(listenUri);
        using var client2 = await ConnectAndHelloAsync(listenUri);

        await SubscribeAsync(client1, Guid.NewGuid(), "/thing/status", "Changed");
        await SubscribeAsync(client2, Guid.NewGuid(), "/thing/status", "Changed");

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(123)));
#pragma warning restore CS0612

        var event1 = await ReceiveUntilAsync<RemoteEventMessage>(client1);
        var event2 = await ReceiveUntilAsync<RemoteEventMessage>(client2);
        Assert.AreEqual(123, event1.EventArg!.Sample.GetVal<int>());
        Assert.AreEqual(123, event2.EventArg!.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_SameSubscriptionAcrossSessions_ShouldAggregateOneLocalSubscriptionAndFanOut()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client1 = await ConnectAndHelloAsync(listenUri);
        using var client2 = await ConnectAndHelloAsync(listenUri);
        var token1 = Guid.NewGuid();
        var token2 = Guid.NewGuid();

        await SubscribeAsync(client1, token1, "/thing/status", "Changed");
        await SubscribeAsync(client2, token2, "/thing/status", "Changed");

        Assert.IsTrue(await WaitUntilAsync(() => GetLocalSubscriptionCount() == 1));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(456)));
#pragma warning restore CS0612

        var event1 = await ReceiveUntilAsync<RemoteEventMessage>(client1);
        var event2 = await ReceiveUntilAsync<RemoteEventMessage>(client2);
        Assert.AreEqual(token1, event1.TokenId);
        Assert.AreEqual(token2, event2.TokenId);
        Assert.AreEqual(456, event1.EventArg!.Sample.GetVal<int>());
        Assert.AreEqual(456, event2.EventArg!.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_MultipleClientsWithConsecutiveDistinctSubscriptions_ShouldDeliverEachEventOnce()
    {
        const int sourceCount = 10;
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri, maxConcurrentFanOutEvents: 128);
        using var client1 = await ConnectAndHelloAsync(listenUri);
        using var client2 = await ConnectAndHelloAsync(listenUri);
        using var client3 = await ConnectAndHelloAsync(listenUri);
        var clients = new[] { client1, client2, client3 };
        var sources = Enumerable.Range(1, sourceCount).Select(index => $"/thing/tick/{index}").ToArray();
        var expectedTokens = new Dictionary<ClientWebSocket, HashSet<Guid>>();

        foreach (var client in clients)
        {
            var tokens = new HashSet<Guid>();
            foreach (var source in sources)
            {
                var tokenId = Guid.NewGuid();
                tokens.Add(tokenId);
                await SubscribeAsync(client, tokenId, source, "Changed");
            }

            expectedTokens.Add(client, tokens);
        }

        Assert.IsTrue(await WaitUntilAsync(() => GetLocalSubscriptionCount() == sourceCount));

#pragma warning disable CS0612
        foreach (var source in sources)
        {
            MyHub.Publish(new EventArg(source, "Changed", new Status<string>(source)));
        }
#pragma warning restore CS0612

        foreach (var client in clients)
        {
            var receivedTokens = new HashSet<Guid>();
            var receivedSources = new HashSet<string>();
            for (var index = 0; index < sourceCount; index++)
            {
                var remoteEvent = await ReceiveUntilAsync<RemoteEventMessage>(client);
                Assert.IsTrue(remoteEvent.TokenId.HasValue);
                receivedTokens.Add(remoteEvent.TokenId.Value);
                receivedSources.Add(remoteEvent.EventArg!.Source);
            }

            CollectionAssert.AreEquivalent(expectedTokens[client].ToArray(), receivedTokens.ToArray());
            CollectionAssert.AreEquivalent(sources, receivedSources.ToArray());
            Assert.AreEqual(sourceCount, receivedTokens.Count, "A client received a duplicate event token.");
            Assert.AreEqual(sourceCount, receivedSources.Count, "A client received a duplicate event source.");
            Assert.IsFalse(await TryReceiveEventAsync(client, TimeSpan.FromMilliseconds(300)));
        }
    }

    [TestMethod]
    public async Task Server_UnmarkedResourceSubscription_ShouldObserveChangedValueWithoutExplicitPublish()
    {
        var thing = new UnmarkedObservedThing();
        MyHub.TryAddThing(thing, "/", "thing");
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);

        await SubscribeAsync(client, Guid.NewGuid(), "/thing/ObservedStatus", "changed");

        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 1));
        Assert.AreEqual(1, GetLocalSubscriptionCount());
        Assert.IsTrue(
            await WaitUntilAsync(() => GetObservedResourceCount() == 1),
            "Expected remote subscription for an existing unmarked resource to start resource observation.");
        Assert.IsInstanceOfType(
            GetServerLocalSubscription(module, "/thing/ObservedStatus", "changed"),
            typeof(ResourceSubscription));

        thing.SetObservedStatus(10);

        var remoteEvent = await TryReceiveUntilAsync<RemoteEventMessage>(client, TimeSpan.FromSeconds(2));
        Assert.IsNotNull(
            remoteEvent,
            "Expected remote subscription to observe an unmarked resource change without explicit Publish.");
        Assert.IsNotNull(remoteEvent.EventArg);
        Assert.AreEqual("/thing/ObservedStatus", remoteEvent.EventArg.Source);
        Assert.AreEqual("changed", remoteEvent.EventArg.EventType);
        Assert.AreEqual(10, remoteEvent.EventArg.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_MissingResourceLikeSource_ShouldRemainPlainEventSubscription()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();

        await SubscribeAsync(client, tokenId, "/thing/MissingStatus", "changed");

        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 1));
        Assert.AreEqual(1, GetLocalSubscriptionCount());
        Assert.AreEqual(0, GetObservedResourceCount());
        Assert.IsInstanceOfType(
            GetServerLocalSubscription(module, "/thing/MissingStatus", "changed"),
            typeof(ResourceGroupSubscription));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/MissingStatus", "changed", new Status<int>(20)));
#pragma warning restore CS0612

        var remoteEvent = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(tokenId, remoteEvent.TokenId);
        Assert.IsNotNull(remoteEvent.EventArg);
        Assert.AreEqual("/thing/MissingStatus", remoteEvent.EventArg.Source);
        Assert.AreEqual("changed", remoteEvent.EventArg.EventType);
        Assert.AreEqual(20, remoteEvent.EventArg.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_NativeEventResourceSubscription_ShouldReceiveExplicitPublishWithoutObservation()
    {
        var thing = new UnmarkedObservedThing();
        MyHub.TryAddThing(thing, "/", "thing");
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);

        await SubscribeAsync(client, Guid.NewGuid(), "/thing/NativeStatus", "changed");

        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 1));
        Assert.AreEqual(1, GetLocalSubscriptionCount());
        Assert.AreEqual(0, GetObservedResourceCount());
        Assert.IsInstanceOfType(
            GetServerLocalSubscription(module, "/thing/NativeStatus", "changed"),
            typeof(ResourceSubscription));

        thing.SetNativeStatusWithoutPublish(10);
        Assert.AreEqual(0, GetObservedResourceCount());

        var nativeResourcePath = MyHub.FindLocalResourceWithPath("/thing/NativeStatus").Path;
#pragma warning disable CS0612
        MyHub.Publish(new EventArg(nativeResourcePath, "changed", new Status<int>(20)));
#pragma warning restore CS0612

        var remoteEvent = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.IsNotNull(remoteEvent.EventArg);
        Assert.AreEqual(nativeResourcePath, remoteEvent.EventArg.Source);
        Assert.AreEqual("changed", remoteEvent.EventArg.EventType);
        Assert.AreEqual(20, remoteEvent.EventArg.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_ResourcePathWithTrailingSegment_ShouldRemainExactEventSubscription()
    {
        var thing = new UnmarkedObservedThing();
        MyHub.TryAddThing(thing, "/", "thing");
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();
        const string parentSource = "/thing/NativeStatus";
        const string eventSource = parentSource + "/2";

        await SubscribeAsync(client, tokenId, eventSource, "changed");

        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 1));
        Assert.AreEqual(1, GetLocalSubscriptionCount());
        Assert.IsInstanceOfType(
            GetServerLocalSubscription(module, eventSource, "changed"),
            typeof(ResourceGroupSubscription));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg(parentSource, "changed", new Status<int>(10)));
#pragma warning restore CS0612

        await Task.Delay(300);

#pragma warning disable CS0612
        MyHub.Publish(new EventArg(eventSource, "changed", new Status<int>(20)));
#pragma warning restore CS0612

        var remoteEvent = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(tokenId, remoteEvent.TokenId);
        Assert.AreEqual(eventSource, remoteEvent.EventArg!.Source);
        Assert.AreEqual(20, remoteEvent.EventArg.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_MethodResourceSubscription_ShouldRejectWithoutPlainEventFallback()
    {
        MyHub.TryAddThing(new UnmarkedObservedThing(), "/", "thing");
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();

        await SendAsync(client, CreateSubscribe(tokenId, "/thing/Method", "changed", 0, 0));
        var ack = (SubscribeAckMessage)await ReceiveAsync(client);

        Assert.AreEqual(tokenId, ack.TokenId);
        Assert.IsFalse(ack.Accepted);
        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 0));
        Assert.AreEqual(0, GetLocalSubscriptionCount());

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/Method", "changed", new Status<int>(30)));
#pragma warning restore CS0612

        Assert.IsFalse(await TryReceiveEventAsync(client, TimeSpan.FromMilliseconds(300)));
    }

    [TestMethod]
    public async Task Server_UnmarkedResourceNonChangedEvent_ShouldRejectWithoutPlainEventFallback()
    {
        MyHub.TryAddThing(new UnmarkedObservedThing(), "/", "thing");
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();

        await SendAsync(client, CreateSubscribe(tokenId, "/thing/ObservedStatus", "alarm", 0, 0));
        var ack = (SubscribeAckMessage)await ReceiveAsync(client);

        Assert.AreEqual(tokenId, ack.TokenId);
        Assert.IsFalse(ack.Accepted);
        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 0));
        Assert.AreEqual(0, GetLocalSubscriptionCount());
        Assert.AreEqual(0, GetObservedResourceCount());

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/ObservedStatus", "alarm", new Status<int>(40)));
#pragma warning restore CS0612

        Assert.IsFalse(await TryReceiveEventAsync(client, TimeSpan.FromMilliseconds(300)));
    }

    [TestMethod]
    public async Task Server_CaseVariantSubscriptionsAcrossSessions_ShouldAggregateOneLocalSubscription()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client1 = await ConnectAndHelloAsync(listenUri);
        using var client2 = await ConnectAndHelloAsync(listenUri);

        await SubscribeAsync(client1, Guid.NewGuid(), "/Thing/Status", "Changed");
        await SubscribeAsync(client2, Guid.NewGuid(), "/thing/status", "changed");

        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 2));
        Assert.AreEqual(1, GetLocalSubscriptionCount());
    }

    [TestMethod]
    public async Task Server_UnsubscribeOneAggregatedSubscriber_ShouldKeepOtherSubscriber()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client1 = await ConnectAndHelloAsync(listenUri);
        using var client2 = await ConnectAndHelloAsync(listenUri);
        var token1 = Guid.NewGuid();
        var token2 = Guid.NewGuid();

        await SubscribeAsync(client1, token1, "/thing/status", "Changed");
        await SubscribeAsync(client2, token2, "/thing/status", "Changed");
        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 2));

        await SendAsync(client1, new UnsubscribeMessage { TokenId = token1 });

        Assert.IsTrue(await WaitUntilAsync(() =>
            GetRemoteSubscriptionCount(module) == 1 &&
            GetLocalSubscriptionCount() == 1));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(789)));
#pragma warning restore CS0612

        Assert.IsFalse(await TryReceiveEventAsync(client1, TimeSpan.FromMilliseconds(300)));
        var event2 = await ReceiveUntilAsync<RemoteEventMessage>(client2);
        Assert.AreEqual(token2, event2.TokenId);
        Assert.AreEqual(789, event2.EventArg!.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_LastAggregatedSubscriberUnsubscribe_ShouldReleaseLocalSubscription()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client1 = await ConnectAndHelloAsync(listenUri);
        using var client2 = await ConnectAndHelloAsync(listenUri);
        var token1 = Guid.NewGuid();
        var token2 = Guid.NewGuid();

        await SubscribeAsync(client1, token1, "/thing/status", "Changed");
        await SubscribeAsync(client2, token2, "/thing/status", "Changed");
        await SendAsync(client1, new UnsubscribeMessage { TokenId = token1 });
        await SendAsync(client2, new UnsubscribeMessage { TokenId = token2 });

        Assert.IsTrue(await WaitUntilAsync(() => GetLocalSubscriptionCount() == 0));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(1)));
#pragma warning restore CS0612

        Assert.IsFalse(await TryReceiveEventAsync(client1, TimeSpan.FromMilliseconds(300)));
        Assert.IsFalse(await TryReceiveEventAsync(client2, TimeSpan.FromMilliseconds(300)));
    }

    [TestMethod]
    public async Task Server_RepeatSubscribeSameToken_ShouldReplaceOldSource()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();

        await SubscribeAsync(client, tokenId, "/thing/old", "Changed");
        await SubscribeAsync(client, tokenId, "/thing/new", "Changed", revision: 1);

        Assert.IsTrue(await WaitUntilAsync(() => GetLocalSubscriptionCount() == 1));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/new", "Changed", new Status<int>(20)));
#pragma warning restore CS0612
        var remoteEvent = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(tokenId, remoteEvent.TokenId);
        Assert.AreEqual(20, remoteEvent.EventArg!.Sample.GetVal<int>());

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/old", "Changed", new Status<int>(10)));
#pragma warning restore CS0612
        Assert.IsFalse(await TryReceiveEventAsync(client, TimeSpan.FromMilliseconds(300)));
    }

    [TestMethod]
    public async Task Server_FailedRevisionMigration_ShouldKeepPreviousSubscription()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();

        await SubscribeAsync(client, tokenId, "/thing/old", "Changed");
        var localTokenIdsBefore = GetLocalSubscriptionTokenIds();

        var invalidUpdateJson = WebSocketMessageSerializer
            .Serialize(CreateSubscribe(tokenId, "/thing/new", "Changed", 0, 1))
            .Replace("/thing/new", "[", StringComparison.Ordinal);
        await SendRawTextAsync(client, invalidUpdateJson);
        var ack = (SubscribeAckMessage)await ReceiveAsync(client);

        Assert.IsFalse(ack.Accepted);
        Assert.AreEqual(1, ack.Revision);
        CollectionAssert.AreEquivalent(localTokenIdsBefore, GetLocalSubscriptionTokenIds());
        Assert.AreEqual(1, GetRemoteSubscriptionCount(module));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/old", "Changed", new Status<int>(10)));
#pragma warning restore CS0612
        var remoteEvent = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(10, remoteEvent.EventArg!.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_DifferentPerformanceLevels_ShouldShareOneLocalSubscriptionAndSendDifferentPayloads()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var fullClient = await ConnectAndHelloAsync(listenUri);
        using var lightweightClient = await ConnectAndHelloAsync(listenUri);

        await SubscribeAsync(fullClient, Guid.NewGuid(), "/thing/status", "Changed", performanceLevel: 0);
        await SubscribeAsync(lightweightClient, Guid.NewGuid(), "/thing/status", "Changed", performanceLevel: 1);

        Assert.IsTrue(await WaitUntilAsync(() => GetLocalSubscriptionCount() == 1));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(123)));
#pragma warning restore CS0612

        var full = await ReceiveUntilAsync<RemoteEventMessage>(fullClient);
        var lightweight = await ReceiveUntilAsync<RemoteEventMessage>(lightweightClient);
        Assert.AreEqual(0, full.EventArg!.PerformanceLevel);
        Assert.AreEqual(1, lightweight.EventArg!.PerformanceLevel);
        Assert.AreEqual(123, full.EventArg.Sample.GetVal<int>());
        Assert.AreEqual(123L, lightweight.EventArg.Sample.GetVal<long>());
        Assert.IsNotNull(full.EventArg.Sample.Timestamp);
        Assert.AreEqual(full.EventArg.Sample.Timestamp, lightweight.EventArg.Sample.Timestamp);
    }

    [TestMethod]
    public async Task Server_PerformanceLevelRevisionUpdate_ShouldKeepLocalSubscriptionToken()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();

        await SubscribeAsync(client, tokenId, "/thing/status", "Changed", performanceLevel: 1);
        var localTokenIdsBefore = GetLocalSubscriptionTokenIds();

        await SubscribeAsync(client, tokenId, "/thing/status", "Changed", performanceLevel: 0, revision: 1);

        CollectionAssert.AreEquivalent(localTokenIdsBefore, GetLocalSubscriptionTokenIds());
#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(456)));
#pragma warning restore CS0612
        var remoteEvent = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(0, remoteEvent.EventArg!.PerformanceLevel);
    }

    [TestMethod]
    public async Task Server_SubscribeRevision_ShouldRejectStaleAndConflictingRequests()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();

        await SubscribeAsync(client, tokenId, "/thing/status", "Changed", revision: 2);

        await SendAsync(client, CreateSubscribe(tokenId, "/thing/old", "Changed", 0, 1));
        var staleAck = (SubscribeAckMessage)await ReceiveAsync(client);
        await SendAsync(client, CreateSubscribe(tokenId, "/thing/other", "Changed", 0, 2));
        var conflictAck = (SubscribeAckMessage)await ReceiveAsync(client);

        Assert.IsFalse(staleAck.Accepted);
        Assert.AreEqual(1, staleAck.Revision);
        Assert.AreEqual("staleRevision", staleAck.Reason);
        Assert.IsFalse(conflictAck.Accepted);
        Assert.AreEqual(2, conflictAck.Revision);
        Assert.AreEqual("revisionConflict", conflictAck.Reason);
        Assert.AreEqual(1, GetLocalSubscriptionCount());
    }

    [TestMethod]
    public async Task Server_SubscribeSameRevisionAndFilter_ShouldBeIdempotent()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();

        await SubscribeAsync(client, tokenId, "/thing/status", "Changed", revision: 3);
        var localTokenIdsBefore = GetLocalSubscriptionTokenIds();
        await SubscribeAsync(client, tokenId, "/thing/status", "Changed", revision: 3);

        CollectionAssert.AreEquivalent(localTokenIdsBefore, GetLocalSubscriptionTokenIds());
        Assert.AreEqual(1, GetLocalSubscriptionCount());
        Assert.AreEqual(1, GetRemoteSubscriptionCount(module));
    }

    [TestMethod]
    public async Task Server_UnsupportedPerformanceLevel_ShouldRejectWithoutCreatingSubscription()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);

        await SendAsync(client, CreateSubscribe(Guid.NewGuid(), "/thing/status", "Changed", 2, 4));
        var ack = (SubscribeAckMessage)await ReceiveAsync(client);

        Assert.IsFalse(ack.Accepted);
        Assert.AreEqual(4, ack.Revision);
        StringAssert.Contains(ack.Reason, "Unsupported PerformanceLevel");
        Assert.AreEqual(0, GetLocalSubscriptionCount());
        Assert.AreEqual(0, GetRemoteSubscriptionCount(module));
    }

    [TestMethod]
    public async Task Server_SameTokenIdAcrossSessions_ShouldUnsubscribeIndependently()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client1 = await ConnectAndHelloAsync(listenUri);
        using var client2 = await ConnectAndHelloAsync(listenUri);
        var sharedTokenId = Guid.NewGuid();

        await SubscribeAsync(client1, sharedTokenId, "/thing/status", "Changed");
        await SubscribeAsync(client2, sharedTokenId, "/thing/status", "Changed");
        await SendAsync(client1, new UnsubscribeMessage { TokenId = sharedTokenId });

        Assert.IsTrue(await WaitUntilAsync(() => GetLocalSubscriptionCount() == 1));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(33)));
#pragma warning restore CS0612

        Assert.IsFalse(await TryReceiveEventAsync(client1, TimeSpan.FromMilliseconds(300)));
        var event2 = await ReceiveUntilAsync<RemoteEventMessage>(client2);
        Assert.AreEqual(sharedTokenId, event2.TokenId);
        Assert.AreEqual(33, event2.EventArg!.Sample.GetVal<int>());
    }

    [TestMethod]
    public async Task Server_MultipleSourcesAndTypes_ShouldUseOrSemanticsAndCleanupAllKeys()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();
        var filter = new EventFilter("/thing/a", "Changed", 0, "ws://127.0.0.1:1/cfet2/event/v2");
        filter.SourcesAndTypes.Add(new SourceAndTypeFilter { Source = "/thing/b", EventType = "Changed" });

        await SendAsync(client, new SubscribeMessage
        {
            TokenId = tokenId,
            Filter = filter,
        });

        var ack = (SubscribeAckMessage)await ReceiveAsync(client);
        Assert.IsTrue(ack.Accepted);
        Assert.IsTrue(await WaitUntilAsync(() => GetLocalSubscriptionCount() == 2));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/a", "Changed", new Status<int>(100)));
#pragma warning restore CS0612
        var first = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(100, first.EventArg!.Sample.GetVal<int>());

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/b", "Changed", new Status<int>(200)));
#pragma warning restore CS0612
        var second = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(200, second.EventArg!.Sample.GetVal<int>());

        await SendAsync(client, new UnsubscribeMessage { TokenId = tokenId });

        Assert.IsTrue(await WaitUntilAsync(() => GetLocalSubscriptionCount() == 0));
    }

    [TestMethod]
    public async Task Server_HighFrequencyPublishAndUnsubscribe_ShouldStopDeliveryAfterCleanup()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();
        await SubscribeAsync(client, tokenId, "/thing/status", "Changed");

        var publishTask = Task.Run(() =>
        {
#pragma warning disable CS0612
            Parallel.For(0, 1_000, index =>
                MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(index))));
#pragma warning restore CS0612
        });
        await SendAsync(client, new UnsubscribeMessage { TokenId = tokenId });
        await publishTask;

        Assert.IsTrue(await WaitUntilAsync(() => GetLocalSubscriptionCount() == 0));
        Assert.AreEqual(0, GetLocalSubscriptionCount());
    }

    [TestMethod]
    public async Task Server_Dispose_ShouldStopAcceptingNewConnections()
    {
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);

        module.Dispose();
        using var client = new ClientWebSocket();
        client.Options.AddSubProtocol(WebSocketProtocolConstants.SubProtocol);

        await AssertThrowsAsync<WebSocketException>(() =>
            client.ConnectAsync(ToWebSocketUri(listenUri), CancellationToken.None));
    }

    [TestMethod]
    public void Server_WssListenUri_ShouldRemainReserved()
    {
        var listenUri = new Uri($"wss://127.0.0.1:{GetFreePort()}/cfet2/event/v2");

        Assert.ThrowsExactly<NotSupportedException>(() =>
        {
            using var module = StartServerModule(listenUri);
        });
    }

    [TestMethod]
    public void Server_InvalidHeartbeatTiming_ShouldFailBeforeListening()
    {
        var listenUri = CreateListenUri();

        var exception = Assert.ThrowsExactly<Jtext103.CFET2.WebSocketCommunicationModule.Exceptions.WebSocketConfigurationException>(() =>
        {
            using var module = StartServerModule(
                listenUri,
                heartbeatInterval: TimeSpan.FromSeconds(30),
                serverIdleTimeout: TimeSpan.FromSeconds(89));
        });

        StringAssert.Contains(exception.Message, "at least 3 times");
    }

    private WebSocketCommunicationModule StartServerModule(
        Uri listenUri,
        int? maxMessageBytes = null,
        TimeSpan? heartbeatInterval = null,
        TimeSpan? serverIdleTimeout = null,
        int? maxServerSessions = null,
        int? maxConcurrentFanOutEvents = null,
        TimeSpan? subscribeAckTimeout = null, int maxSubscribeRetryCount = 3,
        int maxPreparations = 4, int maxPendingPreparations = 256)
    {
        var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions
        {
            EnableServer = true,
            ListenUri = listenUri,
            SubscribeAckTimeout = subscribeAckTimeout ?? TimeSpan.FromSeconds(10),
            MaxSubscribeRetryCount = maxSubscribeRetryCount,
            MaxConcurrentSubscriptionPreparations = maxPreparations,
            MaxPendingSubscriptionPreparations = maxPendingPreparations,
            ReceiveBufferSize = 1024,
            MaxMessageBytes = maxMessageBytes ?? 4 * 1024 * 1024,
            HeartbeatInterval = heartbeatInterval ?? TimeSpan.FromSeconds(30),
            ServerIdleTimeout = serverIdleTimeout ?? TimeSpan.FromSeconds(120),
            MaxServerSessions = maxServerSessions ?? 1024,
            MaxConcurrentFanOutEvents = maxConcurrentFanOutEvents ?? Math.Max(1, Environment.ProcessorCount),
        });

        MyHub.TryAddCommunicationModule(module);
        MyHub.StartCommunication();
        return module;
    }

    private static async Task<ClientWebSocket> ConnectAndHelloAsync(Uri listenUri)
    {
        var client = await ConnectAsync(listenUri);
        await SendAsync(client, new HelloMessage { ClientId = "test-client" });
        var ack = (HelloAckMessage)await ReceiveAsync(client);
        Assert.IsTrue(ack.Accepted);
        return client;
    }

    private static async Task<ClientWebSocket> ConnectAsync(Uri listenUri)
    {
        var client = new ClientWebSocket();
        client.Options.AddSubProtocol(WebSocketProtocolConstants.SubProtocol);
        await client.ConnectAsync(ToWebSocketUri(listenUri), CancellationToken.None);
        return client;
    }

    private static async Task SubscribeAsync(
        ClientWebSocket client,
        Guid tokenId,
        string source,
        string eventType,
        string host = "",
        int performanceLevel = 0,
        int revision = 0)
    {
        await SendAsync(client, CreateSubscribe(tokenId, source, eventType, performanceLevel, revision, host));
        var ack = (SubscribeAckMessage)await ReceiveAsync(client);
        Assert.IsTrue(ack.Accepted);
        Assert.AreEqual(revision, ack.Revision);
    }

    private static SubscribeMessage CreateSubscribe(
        Guid tokenId,
        string source,
        string eventType,
        int performanceLevel,
        int revision,
        string host = "")
    {
        return new SubscribeMessage
        {
            TokenId = tokenId,
            Revision = revision,
            Filter = new EventFilter(
                source,
                eventType,
                performanceLevel,
                string.IsNullOrEmpty(host) ? "ws://127.0.0.1:1/cfet2/event/v2" : host),
        };
    }

    private static async Task SendAsync(ClientWebSocket client, WebSocketMessageEnvelope message)
    {
        var bytes = WebSocketMessageSerializer.SerializeToUtf8Bytes(message);
        await client.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    private static async Task SendRawTextAsync(ClientWebSocket client, string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        await client.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    private static async Task<WebSocketMessageEnvelope> ReceiveAsync(ClientWebSocket client, int timeoutMilliseconds = 3000)
    {
        using var cts = new CancellationTokenSource(timeoutMilliseconds);
        var buffer = new byte[4096];
        using var stream = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await client.ReceiveAsync(buffer, cts.Token);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                throw new WebSocketException("Socket closed.");
            }

            stream.Write(buffer, 0, result.Count);
        }
        while (!result.EndOfMessage);

        return WebSocketMessageSerializer.DeserializeMessage(Encoding.UTF8.GetString(stream.ToArray()));
    }

    private static async Task<T> ReceiveUntilAsync<T>(ClientWebSocket client)
        where T : WebSocketMessageEnvelope
    {
        var started = DateTime.UtcNow;
        while ((DateTime.UtcNow - started).TotalMilliseconds < 3000)
        {
            var message = await ReceiveAsync(client);
            if (message is T typed)
            {
                return typed;
            }
        }

        Assert.Fail($"Expected {typeof(T).Name}.");
        throw new InvalidOperationException();
    }

    private static async Task<T?> TryReceiveUntilAsync<T>(ClientWebSocket client, TimeSpan timeout)
        where T : WebSocketMessageEnvelope
    {
        var started = DateTime.UtcNow;
        while (DateTime.UtcNow - started < timeout)
        {
            var remaining = timeout - (DateTime.UtcNow - started);
            if (remaining <= TimeSpan.Zero)
            {
                return null;
            }

            try
            {
                var message = await ReceiveAsync(client, (int)Math.Max(1, remaining.TotalMilliseconds));
                if (message is T typed)
                {
                    return typed;
                }
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        return null;
    }

    private static async Task<bool> TryReceiveEventAsync(ClientWebSocket client, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var buffer = new byte[4096];
        try
        {
            var result = await client.ReceiveAsync(buffer, cts.Token);
            return result.Count > 0;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private int GetLocalSubscriptionCount()
    {
        var field = typeof(EventHub).GetField("<localSubDict>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(field);
        var dict = (System.Collections.IDictionary)field.GetValue(MyHub.EventHub)!;
        return dict.Count;
    }

    private Guid[] GetLocalSubscriptionTokenIds()
    {
        var field = typeof(EventHub).GetField("<localSubDict>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(field);
        var dict = (System.Collections.IDictionary)field.GetValue(MyHub.EventHub)!;
        return dict.Keys.Cast<Guid>().ToArray();
    }

    private int GetObservedResourceCount()
    {
        var managerField = typeof(EventHub).GetField("resourceObservationManager", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(managerField);
        var manager = managerField.GetValue(MyHub.EventHub);
        Assert.IsNotNull(manager);
        var countProperty = manager.GetType().GetProperty("ObservedResourceCount", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(countProperty);
        return (int)countProperty.GetValue(manager)!;
    }

    private static int GetServerSessionCount(WebSocketCommunicationModule module)
    {
        var serverField = typeof(WebSocketCommunicationModule).GetField("server", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(serverField);
        var server = serverField.GetValue(module);
        Assert.IsNotNull(server);
        var sessionsField = server.GetType().GetField("sessions", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(sessionsField);
        var sessions = sessionsField.GetValue(server)!;
        return (int)sessions.GetType().GetProperty("Count")!.GetValue(sessions)!;
    }

    private static int GetRemoteSubscriptionCount(WebSocketCommunicationModule module)
    {
        var serverField = typeof(WebSocketCommunicationModule).GetField("server", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(serverField);
        var server = serverField.GetValue(module);
        Assert.IsNotNull(server);
        var managerField = server.GetType().GetField("subscriptionManager", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(managerField);
        var manager = managerField.GetValue(server);
        Assert.IsNotNull(manager);
        var remoteIndexField = manager.GetType().GetField("remoteIndex", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(remoteIndexField);
        var remoteIndex = remoteIndexField.GetValue(manager)!;
        return (int)remoteIndex.GetType().GetProperty("Count")!.GetValue(remoteIndex)!;
    }

    private static object GetServerLocalSubscription(
        WebSocketCommunicationModule module,
        string source,
        string eventType)
    {
        var serverField = typeof(WebSocketCommunicationModule).GetField("server", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(serverField);
        var server = serverField.GetValue(module);
        Assert.IsNotNull(server);
        var managerField = server.GetType().GetField("subscriptionManager", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(managerField);
        var manager = managerField.GetValue(server);
        Assert.IsNotNull(manager);
        var aggregatedField = manager.GetType().GetField("aggregatedSubscriptions", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(aggregatedField);
        var aggregated = (System.Collections.IEnumerable)aggregatedField.GetValue(manager)!;

        foreach (var item in aggregated)
        {
            var key = item.GetType().GetProperty("Key")!.GetValue(item)!;
            var value = item.GetType().GetProperty("Value")!.GetValue(item)!;
            var keySource = (string)key.GetType().GetProperty("Source")!.GetValue(key)!;
            var keyEventType = (string)key.GetType().GetProperty("EventType")!.GetValue(key)!;
            if (!string.Equals(keySource, source, StringComparison.OrdinalIgnoreCase) ||
                !string.Equals(keyEventType, eventType, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var localSubscription = value.GetType()
                .GetProperty("LocalSubscription")!
                .GetValue(value);
            Assert.IsNotNull(localSubscription);
            return localSubscription;
        }

        Assert.Fail($"Server local subscription not found: {source} {eventType}");
        throw new InvalidOperationException();
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

    private static Uri CreateListenUri()
    {
        return new Uri($"ws://127.0.0.1:{GetFreePort()}/cfet2/event/v2");
    }

    private static Uri ToWebSocketUri(Uri listenUri)
    {
        return new Uri($"ws://{listenUri.Host}:{listenUri.Port}{listenUri.AbsolutePath}");
    }

    private static Uri ToHttpUri(Uri listenUri)
    {
        return new Uri($"http://{listenUri.Host}:{listenUri.Port}{listenUri.AbsolutePath}");
    }

    private static async Task AssertThrowsAsync<TException>(Func<Task> action)
        where TException : Exception
    {
        try
        {
            await action();
        }
        catch (TException)
        {
            return;
        }

        Assert.Fail($"Expected {typeof(TException).Name}.");
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
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

    [TestMethod]
    public async Task Server_ComplexResource_ShouldRejectObservationButAllowNativeEvents()
    {
        MyHub.TryAddThing(new UnmarkedObservedThing(), "/", "thing");
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var tokenId = Guid.NewGuid();
        await SendAsync(client, CreateSubscribe(tokenId, "/thing/ArrayStatus", "changed", 0, 0));
        var ack = await ReceiveUntilAsync<SubscribeAckMessage>(client);
        Assert.IsFalse(ack.Accepted);
        StringAssert.Contains(ack.Reason!, "/thing/ArrayStatus");
        StringAssert.Contains(ack.Reason!, "System.Object[]");
        StringAssert.Contains(ack.Reason!, "Cfet2Event");
        Assert.AreEqual(0, GetRemoteSubscriptionCount(module));
        Assert.AreEqual(0, GetLocalSubscriptionCount());
        Assert.AreEqual(0, GetObservedResourceCount());

        await SubscribeAsync(client, tokenId, "/thing/NativeArray", "changed", revision: 1);
        Assert.AreEqual(0, GetObservedResourceCount());
        MyHub.PublishResource("/thing/NativeArray", new[] { 7, 8 });
        var message = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(tokenId, message.TokenId);
        CollectionAssert.AreEqual(new[] { 7, 8 }, message.EventArg!.Sample.GetVal<int[]>());
    }

    private sealed class UnmarkedObservedThing : Thing
    {
        [Cfet2Status]
        public object[] ArrayStatus => new object[] { 1 };

        [Cfet2Status, Cfet2Event]
        public int[] NativeArray => new[] { 1 };

        [Cfet2Status]
        public int ObservedStatus { get; private set; } = 1;

        [Cfet2Status]
        [Cfet2Event]
        public int NativeStatus { get; private set; } = 1;

        public void SetObservedStatus(int value)
        {
            ObservedStatus = value;
        }

        public void SetNativeStatusWithoutPublish(int value)
        {
            NativeStatus = value;
        }

        [Cfet2Method]
        public void Method()
        {
        }
    }
}
