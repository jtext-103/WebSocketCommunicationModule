using System.Net.WebSockets;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using NLog;
using NLog.Targets;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed class WebSocketSubscriptionLoggingTests
{
    [TestMethod]
    public void ExistingHandler_ShouldUseLoggerInstalledAfterConstruction()
    {
        var previousLogger = WebSocketLog.Logger;
        var target = new MemoryTarget { Layout = "${message}" };
        var config = new NLog.Config.LoggingConfiguration();
        config.AddRule(LogLevel.Error, LogLevel.Fatal, target);
        using var factory = new LogFactory(config);
        try
        {
            WebSocketLog.Initialize(WebSocketLog.EmptyLogger.Instance);
            var store = new RemoteEndpointSubscriptionStore(RemoteEndpointKey.Parse("ws://localhost:8003/cfet2/event/v2"));
            var token = Guid.NewGuid();
            store.AddOrUpdate(token, 0, new EventFilter("/test", "changed"));
            var handler = new RemoteEndpointMessageHandler(
                store, _ => true, (_, _, _) => Task.CompletedTask, TimeSpan.FromSeconds(1),
                _ => { }, _ => { }, () => false, _ => Assert.Fail());
            using var socket = new ClientWebSocket();
            handler.HandleIncomingMessage(socket, 1, new SubscribeAckMessage { TokenId = token, Accepted = false });
            Assert.IsEmpty(target.Logs);
            WebSocketLog.Initialize(new Cfet2Logger(factory.GetLogger("WebSocket")));
            store.AddOrUpdate(token, 1, new EventFilter("/test", "changed"));
            handler.HandleIncomingMessage(socket, 1, new SubscribeAckMessage { TokenId = token, Revision = 1, Accepted = false });
            Assert.HasCount(1, target.Logs);
            using var module = new WebSocketCommunicationModule(new Jtext103.CFET2.WebSocketCommunicationModule.Options.WebSocketCommunicationOptions());
            Assert.AreNotSame(WebSocketLog.EmptyLogger.Instance, WebSocketLog.Logger);
        }
        finally
        {
            WebSocketLog.Initialize(previousLogger);
        }
    }

    [TestMethod]
    public void RejectedAck_ShouldWriteConsoleAndTextOncePerAttempt()
    {
        var previousConfig = LogManager.Configuration;
        var previousLogger = WebSocketLog.Logger;
        var previousConsole = Console.Out;
        var logPath = Path.Combine(Path.GetTempPath(), $"cfet-subscription-{Guid.NewGuid():N}.txt");
        using var output = new StringWriter();
        try
        {
            Console.SetOut(output);
            // Use the application's actual logging rules; only isolate the output file.
            NlogProvider.Config();
            LogManager.Configuration!.FindTargetByName<FileTarget>("FileTarget")!.FileName = logPath;
            LogManager.ReconfigExistingLoggers();
            WebSocketLog.Initialize(new Cfet2Logger(LogManager.GetLogger("SubscriptionLoggingTest")));
            var endpoint = RemoteEndpointKey.Parse("ws://127.0.0.1:8003/cfet2/event/v2");
            var store = new RemoteEndpointSubscriptionStore(endpoint);
            var token = Guid.NewGuid();
            var filter = new EventFilter("/device/Waveform", "changed");
            store.AddOrUpdate(token, 2, filter);
            var handler = new RemoteEndpointMessageHandler(
                store, _ => true, (_, _, _) => Task.CompletedTask, TimeSpan.FromSeconds(1),
                _ => { }, _ => { }, () => false, _ => Assert.Fail("Connection should remain usable."));
            using var socket = new ClientWebSocket();
            void Ack(Guid id, int revision, bool accepted, string? reason = null) =>
                handler.HandleIncomingMessage(socket, 1, new SubscribeAckMessage
                {
                    TokenId = id, Revision = revision, Accepted = accepted, Reason = reason
                });

            Ack(token, 1, false, "stale");
            Ack(Guid.NewGuid(), 2, false, "unknown");
            Ack(token, 2, true);
            LogManager.Flush();
            Assert.DoesNotContain("WebSocket subscription failed", output.ToString());

            const string reason = "System.Double[] does not support automatic observation; use [Cfet2Event].";
            Ack(token, 2, false, reason);
            Ack(token, 2, false, reason);
            LogManager.Flush();
            var text = ReadActiveLog(logPath);
            foreach (var result in new[] { text, output.ToString() })
            {
                StringAssert.Contains(result, endpoint.ToUri().ToString());
                StringAssert.Contains(result, "/device/Waveform (changed)");
                StringAssert.Contains(result, token.ToString());
                StringAssert.Contains(result, "revision=2");
                StringAssert.Contains(result, reason);
                Assert.AreEqual(1, result.Split("WebSocket subscription failed").Length - 1);
            }
            Assert.AreEqual(SubscriptionState.Failed, store.Get(token)!.State);
            Assert.AreEqual(reason, store.Get(token)!.LastError);

            store.AddOrUpdate(token, 3, filter);
            Ack(token, 2, false, "stale");
            Ack(token, 3, false);
            LogManager.Flush();
            text = ReadActiveLog(logPath);
            Assert.AreEqual(2, text.Split("WebSocket subscription failed").Length - 1);
            StringAssert.Contains(text, "Subscribe rejected.");
        }
        finally
        {
            LogManager.Flush();
            LogManager.Configuration = previousConfig;
            WebSocketLog.Initialize(previousLogger);
            Console.SetOut(previousConsole);
            if (File.Exists(logPath)) File.Delete(logPath);
        }
    }

    private static string ReadActiveLog(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
