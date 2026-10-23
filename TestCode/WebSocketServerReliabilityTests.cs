using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Jtext103.CFET2.WebSocketCommunicationModule.Server;
using Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions;
using Jtext103.CFET2.WebSocketCommunicationModule.Exceptions;
using Newtonsoft.Json;
using NLog;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed class WebSocketServerReliabilityTests : CFET2Host
{
    private ICfet2Logger previousLogger = null!;
    [TestInitialize]
    public void Init()
    {
        previousLogger = WebSocketLog.Logger;
        KillHubIfAlive();
        HubMaster.InjectHubToModule(this);
    }

    [TestCleanup]
    public void Cleanup()
    {
        KillHubIfAlive();
        WebSocketLog.Initialize(previousLogger);
    }

    [TestMethod]
    public async Task Session_NonGracefulClientDisconnect_ShouldNotLeakExpectedWebSocketException()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var socket = new ScriptedWebSocket(
            [WebSocketMessageSerializer.Serialize(new HelloMessage())],
            new WebSocketException("injected non-graceful disconnect"));
        using var session = new WebSocketSession(new WebSocketCommunicationOptions(), manager, socket);

        await session.RunAsync(CancellationToken.None);
    }

    [TestMethod]
    public async Task Session_RemoteEventSendFailure_ShouldRemoveFailedRemoteSubscription()
    {
        var logger = new RecordingLogger();
        WebSocketLog.Initialize(logger);
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        var tokenId = Guid.NewGuid();
        using var socket = new ScriptedWebSocket(
            [
                WebSocketMessageSerializer.Serialize(new HelloMessage()),
                WebSocketMessageSerializer.Serialize(new SubscribeMessage
                {
                    TokenId = tokenId,
                    Filter = new EventFilter("/thing/status", "Changed"),
                }),
            ],
            receiveFailure: null,
            failSendAfter: 2);
        using var session = new WebSocketSession( new WebSocketCommunicationOptions(), manager, socket);
        var runTask = session.RunAsync(CancellationToken.None);

        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(manager) == 1));
#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(42)));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(manager) == 0));
        Assert.AreEqual(1, logger.NetworkSendErrorCount);
        Assert.AreEqual(0, logger.SerializationErrorCount);
        Assert.AreEqual(1, socket.AbortCount);
        session.Dispose();
        await IgnoreCancellationAsync(runTask);
    }

    [TestMethod]
    public async Task FanOut_OneSubscriberSendFailure_ShouldNotBlockHealthySubscriber()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var failedSocket = new ScriptedWebSocket([], receiveFailure: null, failSendAfter: 0);
        using var healthySocket = new ScriptedWebSocket([], receiveFailure: null);
        using var failedSession = new WebSocketSession(
            new WebSocketCommunicationOptions(),
            manager,
            failedSocket);
        using var healthySession = new WebSocketSession(
            new WebSocketCommunicationOptions(),
            manager,
            healthySocket);
        var filter = new EventFilter("/thing/status", "Changed");

        manager.Subscribe(failedSession, Guid.NewGuid(), filter);
        manager.Subscribe(healthySession, Guid.NewGuid(), filter);

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(42)));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => healthySocket.SendCount == 1));
        Assert.AreEqual(1, healthySocket.SendCount);
    }

    [TestMethod]
    public async Task RemoteEventSerializationFailure_ShouldDropEventAndKeepSubscriptionUsable()
    {
        var logger = new RecordingLogger();
        WebSocketLog.Initialize(logger);
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var socket = new ScriptedWebSocket([], receiveFailure: null);
        using var session = new WebSocketSession(
            new WebSocketCommunicationOptions(),
            manager,
            socket);
        var tokenId = Guid.NewGuid();
        manager.Subscribe(
            session,
            tokenId,
            new EventFilter("/thing/status", "Changed", 1, string.Empty));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg(
            "/thing/status",
            "Changed",
            new Status<object>(new SelfReferencingPayload())));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => logger.SerializationErrorCount == 1));
        Assert.AreEqual(0, socket.SendCount);
        Assert.AreEqual(1, GetRemoteSubscriptionCount(manager));
        StringAssert.Contains(logger.LastSerializationError, $"token={tokenId}");
        StringAssert.Contains(logger.LastSerializationError, $"session={session.Id}");
        StringAssert.Contains(logger.LastSerializationError, "performanceLevel=1");
        StringAssert.Contains(logger.LastSerializationError, "source=/thing/status");
        StringAssert.Contains(logger.LastSerializationError, "eventType=Changed");
        Assert.IsFalse(logger.LastSerializationError.Contains("sensitive-value", StringComparison.Ordinal));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(42)));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => socket.SendCount == 1));
        Assert.AreEqual(1, GetRemoteSubscriptionCount(manager));
    }

    [TestMethod]
    public async Task Level0RemoteEventSerializationFailure_ShouldDropEventAndKeepSubscriptionUsable()
    {
        var logger = new RecordingLogger();
        WebSocketLog.Initialize(logger);
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var socket = new ScriptedWebSocket([], receiveFailure: null);
        using var session = new WebSocketSession( new WebSocketCommunicationOptions(), manager, socket);
        manager.Subscribe(
            session,
            Guid.NewGuid(),
            new EventFilter("/thing/status", "Changed", 0, string.Empty));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg(
            "/thing/status",
            "Changed",
            new Status<object>(new SelfReferencingPayload())));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => logger.SerializationErrorCount == 1));
        Assert.AreEqual(0, socket.SendCount);
        Assert.AreEqual(1, GetRemoteSubscriptionCount(manager));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(42)));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => socket.SendCount == 1));
    }

    [TestMethod]
    public void RecoverableSerializationExceptionClassification_ShouldMatchWhitelistOnly()
    {
        Assert.IsTrue(IsRecoverableSerializationException(new JsonSerializationException("json")));
        Assert.IsTrue(IsRecoverableSerializationException(new NotSupportedException("unsupported")));
        Assert.IsTrue(IsRecoverableSerializationException(new OutboundMessageTooLargeException("large")));
        Assert.IsFalse(IsRecoverableSerializationException(new InvalidOperationException("bug")));
        Assert.IsFalse(IsRecoverableSerializationException(new OutOfMemoryException("fatal")));
    }

    [TestMethod]
    public async Task OversizedRemoteEvent_ShouldDropEventAndKeepSubscriptionUsable()
    {
        var logger = new RecordingLogger();
        WebSocketLog.Initialize(logger);
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var socket = new ScriptedWebSocket([], receiveFailure: null);
        using var session = new WebSocketSession(
            new WebSocketCommunicationOptions { MaxMessageBytes = 512 },
            manager,
            socket);
        manager.Subscribe(
            session,
            Guid.NewGuid(),
            new EventFilter("/thing/status", "Changed", 1, string.Empty));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<string>(new string('x', 2_000))));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => logger.SerializationErrorCount == 1));
        Assert.AreEqual(0, socket.SendCount);
        Assert.AreEqual(1, GetRemoteSubscriptionCount(manager));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(42)));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => socket.SendCount == 1));
        Assert.AreEqual(1, GetRemoteSubscriptionCount(manager));
    }

    [TestMethod]
    public async Task RemoteEventSendCanceledBySessionDispose_ShouldNotReportNetworkFailure()
    {
        var logger = new RecordingLogger();
        WebSocketLog.Initialize(logger);
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var socket = new ScriptedWebSocket([], receiveFailure: null, blockSendAfter: 0);
        using var session = new WebSocketSession(
            new WebSocketCommunicationOptions(),
            manager,
            socket);
        manager.Subscribe(
            session,
            Guid.NewGuid(),
            new EventFilter("/thing/status", "Changed"));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(42)));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => socket.SendCount == 1));
        session.Dispose();

        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(manager) == 0));
        Assert.AreEqual(0, logger.NetworkSendErrorCount);
        Assert.AreEqual(0, socket.AbortCount);
    }

    [TestMethod]
    public async Task MultipleSubscribersSerializationFailure_ShouldKeepAllSubscriptionsUsable()
    {
        var logger = new RecordingLogger();
        WebSocketLog.Initialize(logger);
        using var manager = new WebSocketSubscriptionManager(MyHub, 2);
        using var socket1 = new ScriptedWebSocket([], receiveFailure: null);
        using var socket2 = new ScriptedWebSocket([], receiveFailure: null);
        using var session1 = new WebSocketSession( new WebSocketCommunicationOptions(), manager, socket1);
        using var session2 = new WebSocketSession( new WebSocketCommunicationOptions(), manager, socket2);
        var filter = new EventFilter("/thing/status", "Changed", 1, string.Empty);
        manager.Subscribe(session1, Guid.NewGuid(), filter);
        manager.Subscribe(session2, Guid.NewGuid(), filter);

#pragma warning disable CS0612
        MyHub.Publish(new EventArg(
            "/thing/status",
            "Changed",
            new Status<object>(new SelfReferencingPayload())));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => logger.SerializationErrorCount == 2));
        Assert.AreEqual(0, socket1.SendCount);
        Assert.AreEqual(0, socket2.SendCount);
        Assert.AreEqual(2, GetRemoteSubscriptionCount(manager));

#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/status", "Changed", new Status<int>(42)));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => socket1.SendCount == 1 && socket2.SendCount == 1));
    }

    [TestMethod]
    public async Task OversizedControlMessage_ShouldFaultRunAndCleanupSession()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        var hello = WebSocketMessageSerializer.Serialize(new HelloMessage());
        using var socket = new ScriptedWebSocket(
            [hello],
            receiveFailure: null);
        using var session = new WebSocketSession(
            new WebSocketCommunicationOptions { MaxMessageBytes = Encoding.UTF8.GetByteCount(hello) },
            manager,
            socket);

        await AssertThrowsAsync<OutboundMessageTooLargeException>(() => session.RunAsync(CancellationToken.None));

        Assert.AreEqual(WebSocketState.Closed, socket.State);
        Assert.AreEqual(0, GetRemoteSubscriptionCount(manager));
        Assert.AreEqual(0, socket.SendCount);
    }

    [TestMethod]
    public void OutboundMessageAtExactSizeLimit_ShouldSerializeSuccessfully()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var socket = new ScriptedWebSocket([], receiveFailure: null);
        var message = new PongMessage();
        var expectedPayload = WebSocketMessageSerializer.SerializeToUtf8Bytes(message);
        using var session = new WebSocketSession(
            new WebSocketCommunicationOptions { MaxMessageBytes = expectedPayload.Length },
            manager,
            socket);

        var actualPayload = InvokeSerializeMessage(session, message);

        CollectionAssert.AreEqual(expectedPayload, actualPayload);
    }

    [TestMethod]
    public async Task Session_HandshakeTimeout_ShouldCloseSession()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        using var socket = new ScriptedWebSocket([], receiveFailure: null);
        using var session = new WebSocketSession(
            new WebSocketCommunicationOptions
            {
                ServerHandshakeTimeout = TimeSpan.FromMilliseconds(50),
            },
            manager,
            socket);

        await session.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(2));

        Assert.AreNotEqual(WebSocketState.Open, socket.State);
    }

    [TestMethod]
    public async Task Session_RemoteEventSendTimeout_ShouldRemoveFailedRemoteSubscription()
    {
        var logger = new RecordingLogger();
        WebSocketLog.Initialize(logger);
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        var tokenId = Guid.NewGuid();
        using var socket = new ScriptedWebSocket(
            [
                WebSocketMessageSerializer.Serialize(new HelloMessage()),
                WebSocketMessageSerializer.Serialize(new SubscribeMessage
                {
                    TokenId = tokenId,
                    Filter = new EventFilter("/thing/timeout", "Changed"),
                }),
            ],
            receiveFailure: null,
            blockSendAfter: 2);
        using var session = new WebSocketSession(
            new WebSocketCommunicationOptions
            {
                ServerSendTimeout = TimeSpan.FromMilliseconds(50),
            },
            manager,
            socket);
        var runTask = session.RunAsync(CancellationToken.None);

        // The scripted socket completes the first two sends without blocking.
        // Wait for HelloAck and SubscribeAck so the third send is the event.
        Assert.IsTrue(await WaitUntilAsync(() =>
            GetRemoteSubscriptionCount(manager) == 1 && socket.SendCount >= 2));
#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/timeout", "Changed", new Status<int>(42)));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(manager) == 0));
        Assert.AreEqual(1, logger.NetworkSendErrorCount);
        Assert.AreEqual(0, logger.SerializationErrorCount);
        Assert.AreEqual(1, socket.AbortCount);
        await IgnoreCancellationAsync(runTask);
    }

    [TestMethod]
    public async Task FanOutOverload_ShouldLogErrorOnceWhenQueueIsFull()
    {
        var logger = new RecordingLogger();
        WebSocketLog.Initialize(logger);
        using var manager = new WebSocketSubscriptionManager(MyHub, 1, 1, maxPendingFanOutEvents: 1);
        var tokenId = Guid.NewGuid();
        using var socket = new ScriptedWebSocket(
            [
                WebSocketMessageSerializer.Serialize(new HelloMessage()),
                WebSocketMessageSerializer.Serialize(new SubscribeMessage
                {
                    TokenId = tokenId,
                    Filter = new EventFilter("/thing/backlog", "Changed"),
                }),
            ],
            receiveFailure: null,
            blockSendAfter: 2);
        using var session = new WebSocketSession( new WebSocketCommunicationOptions(), manager, socket);
        var runTask = session.RunAsync(CancellationToken.None);

        Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(manager) == 1));
#pragma warning disable CS0612
        MyHub.Publish(new EventArg("/thing/backlog", "Changed", new Status<int>(1)));
        MyHub.Publish(new EventArg("/thing/backlog", "Changed", new Status<int>(2)));
        MyHub.Publish(new EventArg("/thing/backlog", "Changed", new Status<int>(3)));
#pragma warning restore CS0612

        Assert.IsTrue(await WaitUntilAsync(() => logger.OverloadErrorCount == 1));
        Assert.AreEqual(1, logger.OverloadErrorCount);

        session.Dispose();
        await IgnoreCancellationAsync(runTask);
    }

    private static int GetRemoteSubscriptionCount(WebSocketSubscriptionManager manager)
    {
        var field = typeof(WebSocketSubscriptionManager).GetField("remoteIndex", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(field);
        var value = field.GetValue(manager)!;
        return (int)value.GetType().GetProperty("Count")!.GetValue(value)!;
    }

    private static bool IsRecoverableSerializationException(Exception exception)
    {
        var method = typeof(WebSocketSession).GetMethod(
            "IsRecoverableSerializationException",
            BindingFlags.Static | BindingFlags.NonPublic);
        Assert.IsNotNull(method);
        return (bool)method.Invoke(null, [exception])!;
    }

    [TestMethod]
    public async Task Session_SubscribeAckSendTimeoutShouldDisposeAndReleaseSubscription()
    {
        using var manager = new WebSocketSubscriptionManager(MyHub, 1);
        var hello = WebSocketMessageSerializer.Serialize(new HelloMessage());
        var subscribe = WebSocketMessageSerializer.Serialize(new SubscribeMessage
        {
            TokenId = Guid.NewGuid(), Filter = new EventFilter("/ack/.*", "alarm")
        });
        using var socket = new ScriptedWebSocket([hello, subscribe], receiveFailure: null, blockSendAfter: 1);
        using var session = new WebSocketSession(new WebSocketCommunicationOptions
        { ServerSendTimeout = TimeSpan.FromMilliseconds(40) }, manager, socket);
        await session.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(3));
        Assert.AreEqual(WebSocketState.Closed, socket.State);
        Assert.AreEqual(0, manager.RemoteSubscriptionCount);
        Assert.AreEqual(0, manager.AggregatedSubscriptionCount);
    }

    private static byte[] InvokeSerializeMessage(WebSocketSession session, WebSocketMessageEnvelope message)
    {
        var method = typeof(WebSocketSession).GetMethod(
            "SerializeMessage",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(method);
        return (byte[])method.Invoke(session, [message])!;
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

    private static async Task IgnoreCancellationAsync(Task task)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
        }
    }

    private sealed class ScriptedWebSocket : WebSocket
    {
        private readonly Queue<byte[]> incoming;
        private readonly Exception? receiveFailure;
        private readonly int? failSendAfter;
        private readonly int? blockSendAfter;
        private int sendCount;
        private int abortCount;
        private WebSocketState state = WebSocketState.Open;

        public ScriptedWebSocket(
            IEnumerable<string> incoming,
            Exception? receiveFailure,
            int? failSendAfter = null,
            int? blockSendAfter = null)
        {
            this.incoming = new Queue<byte[]>(incoming.Select(Encoding.UTF8.GetBytes));
            this.receiveFailure = receiveFailure;
            this.failSendAfter = failSendAfter;
            this.blockSendAfter = blockSendAfter;
        }

        public override WebSocketCloseStatus? CloseStatus => null;

        public override string? CloseStatusDescription => null;

        public override string? SubProtocol => WebSocketProtocolConstants.SubProtocol;

        public override WebSocketState State => state;

        public int SendCount => Volatile.Read(ref sendCount);

        public int AbortCount => Volatile.Read(ref abortCount);

        public override void Abort()
        {
            Interlocked.Increment(ref abortCount);
            state = WebSocketState.Aborted;
        }

        public override Task CloseAsync(
            WebSocketCloseStatus closeStatus,
            string? statusDescription,
            CancellationToken cancellationToken)
        {
            state = WebSocketState.Closed;
            return Task.CompletedTask;
        }

        public override Task CloseOutputAsync(
            WebSocketCloseStatus closeStatus,
            string? statusDescription,
            CancellationToken cancellationToken)
        {
            state = WebSocketState.CloseSent;
            return Task.CompletedTask;
        }

        public override void Dispose()
        {
            state = WebSocketState.Closed;
        }

        public override Task<WebSocketReceiveResult> ReceiveAsync(
            ArraySegment<byte> buffer,
            CancellationToken cancellationToken)
        {
            if (incoming.TryDequeue(out var bytes))
            {
                bytes.CopyTo(buffer.Array!, buffer.Offset);
                return Task.FromResult(new WebSocketReceiveResult(bytes.Length, WebSocketMessageType.Text, true));
            }

            if (receiveFailure != null)
            {
                return Task.FromException<WebSocketReceiveResult>(receiveFailure);
            }

            return WaitForCancellationAsync(cancellationToken);
        }

        public override Task SendAsync(
            ArraySegment<byte> buffer,
            WebSocketMessageType messageType,
            bool endOfMessage,
            CancellationToken cancellationToken)
        {
            var currentSendCount = Interlocked.Increment(ref sendCount);
            if (failSendAfter.HasValue && currentSendCount > failSendAfter.Value)
            {
                return Task.FromException(new WebSocketException("injected send failure"));
            }

            if (blockSendAfter.HasValue && currentSendCount > blockSendAfter.Value)
            {
                return Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            return Task.CompletedTask;
        }

        private static async Task<WebSocketReceiveResult> WaitForCancellationAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            throw new OperationCanceledException(cancellationToken);
        }
    }

    private sealed class RecordingLogger : ICfet2Logger
    {
        private int overloadErrorCount;
        private int networkSendErrorCount;
        private int serializationErrorCount;
        private string? lastSerializationError;

        public int OverloadErrorCount => Volatile.Read(ref overloadErrorCount);

        public int SerializationErrorCount => Volatile.Read(ref serializationErrorCount);

        public int NetworkSendErrorCount => Volatile.Read(ref networkSendErrorCount);

        public string LastSerializationError => lastSerializationError ?? string.Empty;

        public void Trace(string message) { }

        public void Debug(string message) { }

        public void Info(string message) { }

        public void Warn(string message) { }

        public void Error(string message)
        {
            if (message.Contains("WebSocket fan-out overload", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref overloadErrorCount);
            }

            if (message.Contains("Remote event serialization failed", StringComparison.Ordinal))
            {
                lastSerializationError = message;
                Interlocked.Increment(ref serializationErrorCount);
            }

            if (message.Contains("Remote event network send failed", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref networkSendErrorCount);
            }
        }

        public void Fatal(string message) { }
    }

    private sealed class SelfReferencingPayload
    {
        public string Sensitive => "sensitive-value";

        public SelfReferencingPayload Self => this;
    }
}
