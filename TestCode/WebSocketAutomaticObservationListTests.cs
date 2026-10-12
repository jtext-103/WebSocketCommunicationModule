using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Config;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

public sealed partial class WebSocketServerPhase7Tests
{
    [TestMethod]
    [DataRow(false, 0)]
    [DataRow(false, 1)]
    [DataRow(true, 0)]
    [DataRow(true, 1)]
    public async Task AutomaticListsDeniedExactAndRegexKeepActiveEventsWithoutPolling(bool regex, int level)
    {
        var saved = GlobalConfig.Get();
        try
        {
            KillHubIfAlive();
            GlobalConfig.AutoDenyList = new[] { new ObservationListRule(path: "/device/Temperature") };
            GlobalConfig.AutoObserveList = new[] { new ObservationListRule(path: "/device/Temperature") };
            GlobalConfig.ResourceObservationIntervalMs = 30;
            HubMaster.InjectHubToModule(this);
            var device = new RegexDevice(); MyHub.TryAddThing(device, "/", "device");
            var listen = CreateListenUri(); using var module = StartServerModule(listen);
            using var client = await ConnectAndHelloAsync(listen);
            var id = Guid.NewGuid();
            await SendAsync(client, CreateSubscribe(id, regex ? "/device/Temp.*" : "/device/Temperature", "changed", level, 0));
            Assert.IsTrue((await ReceiveUntilAsync<SubscribeAckMessage>(client)).Accepted);
            MyHub.StartThings();
            Assert.AreEqual(ResourceObservationStatus.Skipped, MyHub.AutoObservationResults.Single().Status);
            Assert.AreEqual(0, GetObservedResourceCount());
            Assert.AreEqual(regex ? 0 : 1, device.Reads);
            var timestamp = DateTimeOffset.UtcNow.AddMinutes(-2);
            MyHub.PublishResource("/device/Temperature", new Status<int>(81) { Timestamp = timestamp });
            var message = await ReceiveUntilAsync<RemoteEventMessage>(client);
            Assert.AreEqual(81, message.EventArg!.Sample.GetVal<int>());
            Assert.AreEqual(timestamp, message.EventArg.Sample.Timestamp);
            await SendAsync(client, new UnsubscribeMessage { TokenId = id });
            Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 0 && GetLocalSubscriptionCount() == 0));
            Assert.AreEqual(0, GetObservedResourceCount());
        }
        finally { KillHubIfAlive(); GlobalConfig.Set(saved); }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task AutomaticListsResidentObservationSurvivesWireUnsubscribeAndSessionClose(bool abortSession)
    {
        var saved = GlobalConfig.Get();
        try
        {
            KillHubIfAlive();
            GlobalConfig.AutoDenyList = Array.Empty<ObservationListRule>();
            GlobalConfig.AutoObserveList = new[] { new ObservationListRule(path: "/device/Temperature") };
            GlobalConfig.ResourceObservationIntervalMs = 30;
            HubMaster.InjectHubToModule(this);
            var device = new RegexDevice(); MyHub.TryAddThing(device, "/", "device");
            var listen = CreateListenUri(); using var module = StartServerModule(listen);
            MyHub.StartThings();
            Assert.AreEqual(1, GetObservedResourceCount());
            Assert.AreEqual(0, GetLocalSubscriptionCount());
            using var client = await ConnectAndHelloAsync(listen);
            var id = Guid.NewGuid();
            await SubscribeAsync(client, id, "/device/Temp.*", "changed");
            Assert.AreEqual(1, GetObservedResourceCount());
            device.Value = 71;
            Assert.AreEqual(71, (await ReceiveUntilAsync<RemoteEventMessage>(client)).EventArg!.Sample.GetVal<int>());
            if (abortSession) client.Abort();
            else await SendAsync(client, new UnsubscribeMessage { TokenId = id });
            Assert.IsTrue(await WaitUntilAsync(() => GetRemoteSubscriptionCount(module) == 0 && GetLocalSubscriptionCount() == 0));
            Assert.AreEqual(1, GetObservedResourceCount());
            var reads = device.Reads;
            Assert.IsTrue(await WaitUntilAsync(() => device.Reads >= reads + 2));
            MyHub.DisposeThings();
            Assert.AreEqual(0, GetObservedResourceCount());
        }
        finally { KillHubIfAlive(); GlobalConfig.Set(saved); }
    }
}
