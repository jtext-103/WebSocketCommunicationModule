using System.Collections.Concurrent;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using System.Net.WebSockets;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed class WebSocketRemoteEndpointClientTests
{
    private ICfet2Logger previousLogger = null!;

    [TestInitialize]
    public void SaveLogger() => previousLogger = WebSocketLog.Logger;

    [TestCleanup]
    public void RestoreLogger() => WebSocketLog.Initialize(previousLogger);
    [TestMethod]
    public void ReconnectPolicy_ShouldUseFastRetriesThenExponentialBackoffUntilMax()
    {
        var policy = new ReconnectPolicy
        {
            InitialDelay = TimeSpan.FromSeconds(1),
            FastRetryCount = 5,
            BackoffMultiplier = 2,
            MaxDelay = TimeSpan.FromSeconds(30),
        };

        CollectionAssert.AreEqual(
            new[]
            {
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(4),
                TimeSpan.FromSeconds(8),
                TimeSpan.FromSeconds(16),
                TimeSpan.FromSeconds(30),
                TimeSpan.FromSeconds(30),
            },
            Enumerable.Range(1, 11).Select(policy.GetDelay).ToArray());
    }

    [TestMethod]
    public void ReconnectPolicy_WithZeroDelay_ShouldReturnZero()
    {
        var policy = new ReconnectPolicy
        {
            InitialDelay = TimeSpan.Zero,
            MaxDelay = TimeSpan.FromSeconds(1),
        };

        Assert.AreEqual(TimeSpan.Zero, policy.GetDelay(1));
    }

    [TestMethod]
    public void HeartbeatNegotiation_ShouldUseShorterPositiveInterval()
    {
        Assert.AreEqual(
            TimeSpan.FromMilliseconds(100),
            RemoteEndpointConnection.ResolveHeartbeatInterval(TimeSpan.FromSeconds(1), 100));
        Assert.AreEqual(
            TimeSpan.FromMilliseconds(100),
            RemoteEndpointConnection.ResolveHeartbeatInterval(TimeSpan.FromMilliseconds(100), 1000));
    }

    [TestMethod]
    public void HeartbeatNegotiation_WithInvalidServerInterval_ShouldUseClientInterval()
    {
        Assert.AreEqual(
            TimeSpan.FromMilliseconds(100),
            RemoteEndpointConnection.ResolveHeartbeatInterval(TimeSpan.FromMilliseconds(100), 0));
    }

    [TestMethod]
    public void HeartbeatNegotiation_WithDisabledClientInterval_ShouldUseServerInterval()
    {
        Assert.AreEqual(
            TimeSpan.FromMilliseconds(100),
            RemoteEndpointConnection.ResolveHeartbeatInterval(TimeSpan.Zero, 100));
    }

    [TestMethod]
    public void HeartbeatTimeoutNegotiation_ShouldUseShorterPositiveTimeout()
    {
        Assert.AreEqual(
            TimeSpan.FromMilliseconds(300),
            RemoteEndpointConnection.ResolveHeartbeatTimeout(TimeSpan.FromSeconds(1), 300));
        Assert.AreEqual(
            TimeSpan.FromMilliseconds(300),
            RemoteEndpointConnection.ResolveHeartbeatTimeout(TimeSpan.FromMilliseconds(300), 1000));
    }

    [TestMethod]
    public void HeartbeatTimeoutNegotiation_WithMissingServerTimeout_ShouldUseClientTimeout()
    {
        Assert.AreEqual(
            TimeSpan.FromMilliseconds(300),
            RemoteEndpointConnection.ResolveHeartbeatTimeout(TimeSpan.FromMilliseconds(300), null));
    }

    [TestMethod]
    public void HeartbeatTiming_ShouldRequireTimeoutAtLeastThreeTimesInterval()
    {
        Assert.IsTrue(HeartbeatTiming.IsValid(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(3)));
        Assert.IsFalse(HeartbeatTiming.IsValid(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMilliseconds(2999)));
    }

    [TestMethod]
    public void AddOrUpdateSubscription_ShouldCreatePendingSubscription()
    {
        using var client = CreateClient();
        var tokenId = Guid.NewGuid();
        var filter = new EventFilter("/thing/status", "Changed", 0, client.Endpoint.NormalizedHost);

        client.AddOrUpdateSubscription(tokenId, filter);

        var subscription = client.GetSubscription(tokenId);
        Assert.IsNotNull(subscription);
        Assert.AreEqual(tokenId, subscription.TokenId);
        Assert.AreEqual(client.Endpoint, subscription.Endpoint);
        Assert.AreEqual(SubscriptionState.Pending, subscription.State);
        Assert.AreEqual(filter, subscription.Filter);
        Assert.AreEqual(1, client.SubscriptionCount);
        Assert.IsFalse(client.IsIdle);
    }

    [TestMethod]
    public void AddOrUpdateSubscription_WithExistingToken_ShouldResetToPending()
    {
        using var client = CreateClient();
        var tokenId = Guid.NewGuid();
        var firstFilter = new EventFilter("/thing/old", "Changed", 0, client.Endpoint.NormalizedHost);
        var secondFilter = new EventFilter("/thing/new", "Changed", 0, client.Endpoint.NormalizedHost);

        client.AddOrUpdateSubscription(tokenId, firstFilter);
        client.GetSubscription(tokenId)!.State = SubscriptionState.Active;
        client.AddOrUpdateSubscription(tokenId, secondFilter);

        var subscription = client.GetSubscription(tokenId);
        Assert.IsNotNull(subscription);
        Assert.AreEqual(SubscriptionState.Pending, subscription.State);
        Assert.AreEqual(secondFilter, subscription.Filter);
        Assert.AreEqual(1, client.SubscriptionCount);
    }

    [TestMethod]
    public void RemoveSubscription_ShouldRemoveSubscriptionWithoutRaisingIdle()
    {
        using var client = CreateClient();
        var tokenId = Guid.NewGuid();
        var idleRaised = false;
        client.Idle += _ => idleRaised = true;
        client.AddOrUpdateSubscription(
            tokenId,
            new EventFilter("/thing/status", "Changed", 0, client.Endpoint.NormalizedHost));

        var removed = client.RemoveSubscription(tokenId);

        Assert.IsTrue(removed);
        Assert.IsNull(client.GetSubscription(tokenId));
        Assert.AreEqual(0, client.SubscriptionCount);
        Assert.IsTrue(client.IsIdle);
        Assert.IsFalse(idleRaised);
    }

    [TestMethod]
    public void Dispose_ShouldClearSubscriptionsAndSuppressIdleEvent()
    {
        var client = CreateClient();
        var tokenId = Guid.NewGuid();
        var idleRaised = false;
        client.Idle += _ => idleRaised = true;
        client.AddOrUpdateSubscription(
            tokenId,
            new EventFilter("/thing/status", "Changed", 0, client.Endpoint.NormalizedHost));

        client.Dispose();

        Assert.AreEqual(RemoteConnectionState.Disposed, client.State);
        Assert.AreEqual(0, client.SubscriptionCount);
        Assert.IsTrue(client.IsIdle);
        Assert.IsFalse(idleRaised);
    }

    [TestMethod]
    public async Task EnqueueSubscribeThenUnsubscribe_ShouldApplyActionsInOrder()
    {
        using var client = CreateClient();
        var tokenId = Guid.NewGuid();
        var filter = new EventFilter("/thing/status", "Changed", 0, client.Endpoint.NormalizedHost);

        client.EnqueueSubscribe(tokenId, filter);
        Assert.IsTrue(await WaitUntilAsync(() => client.GetSubscription(tokenId) != null));

        client.EnqueueUnsubscribe(tokenId);

        Assert.IsTrue(await WaitUntilAsync(() => client.GetSubscription(tokenId) == null));
        Assert.AreEqual(0, client.SubscriptionCount);
    }

    [TestMethod]
    public void EnqueueAfterDispose_ShouldNotRecreateSubscription()
    {
        var client = CreateClient();
        var tokenId = Guid.NewGuid();

        client.Dispose();
        client.EnqueueSubscribe(
            tokenId,
            new EventFilter("/thing/status", "Changed", 0, client.Endpoint.NormalizedHost));

        Assert.IsNull(client.GetSubscription(tokenId));
        Assert.AreEqual(0, client.SubscriptionCount);
    }

    [TestMethod]
    public async Task ActiveConnectionOperationsAndDispose_ShouldCompleteWithoutUnexpectedExceptions()
    {
        for (var iteration = 0; iteration < 20; iteration++)
        {
            var client = CreateClient();
            var errors = new ConcurrentQueue<Exception>();
            var workers = Enumerable.Range(0, 4)
                .Select(worker => Task.Run(() =>
                {
                    for (var index = 0; index < 100; index++)
                    {
                        try
                        {
                            var tokenId = Guid.NewGuid();
                            client.AddOrUpdateSubscription(
                                tokenId,
                                new EventFilter($"/thing/{worker}/{index}", "Changed", 0, client.Endpoint.NormalizedHost));
                            client.RemoveSubscription(tokenId);
                        }
                        catch (Exception ex)
                        {
                            errors.Enqueue(ex);
                        }
                    }
                }))
                .ToArray();

            await Task.WhenAll(workers.Append(Task.Run(client.Dispose)));

            Assert.IsEmpty(errors, string.Join(Environment.NewLine, errors.Select(error => error.ToString())));
            Assert.AreEqual(RemoteConnectionState.Disposed, client.State);
        }
    }

    [TestMethod]
    public async Task ActionProcessor_UnexpectedActionException_ShouldContinueProcessingLaterActions()
    {
        var processed = new ConcurrentQueue<Guid>();
        var firstTokenId = Guid.NewGuid();
        var secondTokenId = Guid.NewGuid();
        var throwFirst = true;
        using var store = new DisposableSubscriptionStore(
            new RemoteEndpointSubscriptionStore(RemoteEndpointKey.Parse("ws://127.0.0.1:1/cfet2/event/v2")));
        using var processor = new RemoteEndpointActionProcessor(
            () => false,
            (tokenId, _) =>
            {
                if (throwFirst)
                {
                    throwFirst = false;
                    throw new InvalidOperationException("injected action failure");
                }

                processed.Enqueue(tokenId);
            },
            _ => true,
            () => Task.CompletedTask,
            _ => Task.CompletedTask,
            store.Value);

        processor.EnqueueSubscribe(firstTokenId, new EventFilter("/thing/first", "Changed"));
        processor.EnqueueSubscribe(secondTokenId, new EventFilter("/thing/second", "Changed"));

        Assert.IsTrue(await WaitUntilAsync(() => processed.Contains(secondTokenId)));
    }

    [TestMethod]
    public void SubscriptionStore_WithOnlyFailedSubscriptions_ShouldBeEffectivelyIdle()
    {
        var tokenId = Guid.NewGuid();
        var store = new RemoteEndpointSubscriptionStore(
            RemoteEndpointKey.Parse("ws://127.0.0.1:1/cfet2/event/v2"));
        store.AddOrUpdate(tokenId, new EventFilter("/thing/status", "Changed"));

        store.MarkSubscribeAck(new SubscribeAckMessage
        {
            TokenId = tokenId,
            Accepted = false,
            Reason = "rejected",
        });

        Assert.IsTrue(store.IsEffectivelyIdle);
    }

    [TestMethod]
    public void SubscriptionStore_StaleAck_ShouldNotOverrideNewerRevision()
    {
        var endpoint = RemoteEndpointKey.Parse("ws://127.0.0.1:1/cfet2/event/v2");
        var store = new RemoteEndpointSubscriptionStore(endpoint);
        var tokenId = Guid.NewGuid();
        store.AddOrUpdate(tokenId, 1, new EventFilter("/thing/status", "Changed", 1));
        store.AddOrUpdate(tokenId, 2, new EventFilter("/thing/status", "Changed", 0));

        store.MarkSubscribeAck(new SubscribeAckMessage
        {
            TokenId = tokenId,
            Revision = 1,
            Accepted = false,
            Reason = "stale rejection",
        });

        Assert.AreEqual(2, store.Get(tokenId)!.Revision);
        Assert.AreEqual(SubscriptionState.Pending, store.Get(tokenId)!.State);
        Assert.IsNull(store.Get(tokenId)!.LastError);
    }

    [TestMethod]
    public void SubscriptionStore_AckTimeout_ShouldPrepareSameTokenForRetry()
    {
        var tokenId = Guid.NewGuid();
        var filter = new EventFilter("/thing/status", "Changed");
        var store = new RemoteEndpointSubscriptionStore(
            RemoteEndpointKey.Parse("ws://127.0.0.1:1/cfet2/event/v2"));
        store.AddOrUpdate(tokenId, filter);
        Assert.IsTrue(store.TryMarkSubscribeSending(tokenId));
        store.MarkSubscribeSent(tokenId, DateTime.UtcNow - TimeSpan.FromSeconds(2));

        CollectionAssert.Contains(
            store.GetSubscribeAckTimeouts(DateTime.UtcNow, TimeSpan.FromSeconds(1)).ToArray(),
            tokenId);
        Assert.IsTrue(store.TryPrepareSubscribeRetry(tokenId, 3, out var retryFilter));

        Assert.AreSame(filter, retryFilter);
        Assert.AreEqual(1, store.Get(tokenId)!.SubscribeRetryCount);
    }

    [TestMethod]
    public async Task ActionQueue_OverThreshold_ShouldLogOnce()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new QueueRecordingLogger();
        WebSocketLog.Initialize(logger);
        var endpoint = RemoteEndpointKey.Parse("ws://127.0.0.1:1/cfet2/event/v2");
        using var store = new DisposableSubscriptionStore(new RemoteEndpointSubscriptionStore(endpoint));
        using var processor = new RemoteEndpointActionProcessor(
            () => false,
            (_, _) => { },
            _ => true,
            () => release.Task,
            _ => Task.CompletedTask,
            store.Value,
            endpoint,
            1);

        processor.EnqueueSubscribe(Guid.NewGuid(), new EventFilter("/thing/first", "Changed"));
        await Task.Delay(50);
        processor.EnqueueSubscribe(Guid.NewGuid(), new EventFilter("/thing/second", "Changed"));
        processor.EnqueueSubscribe(Guid.NewGuid(), new EventFilter("/thing/third", "Changed"));
        processor.EnqueueSubscribe(Guid.NewGuid(), new EventFilter("/thing/fourth", "Changed"));

        Assert.IsTrue(await WaitUntilAsync(() => logger.WarningCount == 1));
        Assert.AreEqual(1, logger.WarningCount);
        release.TrySetResult();
    }

    [TestMethod]
    public async Task MessageHandler_PongSendFailure_ShouldReportConnectionFailure()
    {
        var failure = new TaskCompletionSource<Exception>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new RemoteEndpointSubscriptionStore(
            RemoteEndpointKey.Parse("ws://127.0.0.1:1/cfet2/event/v2"));
        using var socket = new ClientWebSocket();
        var handler = new RemoteEndpointMessageHandler(
            store,
            _ => true,
            (_, _, _) => Task.FromException(new WebSocketException("pong failed")),
            TimeSpan.FromSeconds(1),
            _ => { },
            _ => { },
            () => false,
            ex => failure.TrySetResult(ex));

        handler.HandleIncomingMessage(socket, 1, new PingMessage());

        var completed = await Task.WhenAny(failure.Task, Task.Delay(TimeSpan.FromSeconds(3)));
        Assert.AreSame(failure.Task, completed);
        Assert.IsInstanceOfType<WebSocketException>(await failure.Task);
    }

    private static RemoteEndpointClient CreateClient()
    {
        return new RemoteEndpointClient(
            RemoteEndpointKey.Parse("ws://127.0.0.1:1/cfet2/event/v2"),
            new WebSocketCommunicationOptions());
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

    private sealed class NullLogger : ICfet2Logger
    {
        public void Trace(string message)
        {
        }

        public void Debug(string message)
        {
        }

        public void Info(string message)
        {
        }

        public void Warn(string message)
        {
        }

        public void Error(string message)
        {
        }

        public void Fatal(string message)
        {
        }
    }

    private sealed class QueueRecordingLogger : ICfet2Logger
    {
        private int warningCount;

        public int WarningCount => Volatile.Read(ref warningCount);

        public void Trace(string message) { }
        public void Debug(string message) { }
        public void Info(string message) { }
        public void Warn(string message) { }
        public void Fatal(string message) { }

        public void Error(string message)
        {
            if (message.Contains("WebSocket action queue warning", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref warningCount);
            }
        }
    }

    private sealed class DisposableSubscriptionStore : IDisposable
    {
        public DisposableSubscriptionStore(RemoteEndpointSubscriptionStore value)
        {
            Value = value;
        }

        public RemoteEndpointSubscriptionStore Value { get; }

        public void Dispose()
        {
            Value.Clear();
        }
    }
}
