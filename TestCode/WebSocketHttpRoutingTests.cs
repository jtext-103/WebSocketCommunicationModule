using System.Collections.Concurrent;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Attributes;
using Jtext103.CFET2.Core.Config;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;

namespace TestCode;

public sealed partial class WebSocketServerPhase7Tests
{
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task HttpRoutingWithNonDefaultOffsetReceivesParameterizedEventsAndUnsubscribes(bool regex)
    {
        var saved = GlobalConfig.Get();
        try
        {
            KillHubIfAlive();
            GlobalConfig.WebSocketPortOffset = 7;
            GlobalConfig.EventPublishTransports = new[] { EventTransport.WebSocket };
            GlobalConfig.HttpEventSubscriptionTransport = EventTransport.WebSocket;
            HubMaster.InjectHubToModule(this);
            var device = new RoutingDevice(); MyHub.TryAddThing(device, "/", "routing");
            var listen = CreateListenUri();
            var http = new UriBuilder("http", listen.Host, listen.Port - 7).Uri;
            Assert.AreEqual(listen.Port, MyHub.ActiveEventCommunicationConfiguration.GetWebSocketPort(http.Port));
            using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions { ListenUri = listen, EnableServer = true });
            MyHub.TryAddCommunicationModule(module); MyHub.StartCommunication();
            GlobalConfig.WebSocketPortOffset = 1; // routing keeps Core creation snapshot
            Assert.AreEqual(listen.GetLeftPart(UriPartial.Authority), module.GetFullHost("http", http.Host, http.Port));
            Assert.AreEqual("ws://127.0.0.1:9000", module.GetFullHost("ws", "127.0.0.1", 9000));
            Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => module.GetFullHost("http", http.Host, 65535));
            var received = new ConcurrentQueue<EventArg>();
            using var token = MyHub.Subscribe(new EventFilter(http.GetLeftPart(UriPartial.Authority) + (regex ? "/routing/Value/.*" : "/routing/Value/2"), "changed"), received.Enqueue);
            Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().ServerRemoteSubscriptionCount == 1));
            var timestamp = new DateTimeOffset(2026, 10, 13, 0, 0, 0, TimeSpan.Zero);
            Assert.IsTrue(device.Send(2, 42, timestamp));
            Assert.IsTrue(await WaitUntilAsync(() => !received.IsEmpty));
            Assert.IsTrue(received.TryDequeue(out var value));
            Assert.AreEqual("/routing/Value/2", value!.Source);
            Assert.AreEqual(42, value.Sample.GetVal<int>());
            Assert.AreEqual(timestamp, value.Sample.Timestamp);
            Assert.AreEqual(listen.GetLeftPart(UriPartial.Authority), value.Host);
            Assert.AreEqual(0, device.Reads);
            token.Dispose();
            Assert.IsTrue(await WaitUntilAsync(() => module.GetStatus().ServerRemoteSubscriptionCount == 0));
            Assert.IsTrue(received.IsEmpty);
        }
        finally { KillHubIfAlive(); GlobalConfig.Set(saved); }
    }

    private sealed class RoutingDevice : Thing
    {
        public int Reads;
        [Cfet2Status] public int Value(int channel) { Reads++; return channel; }
        public bool Send(int channel, int value, DateTimeOffset timestamp) =>
            PublishResourceIfChanged(nameof(Value), value, new object[] { channel }, timestamp);
    }
}
