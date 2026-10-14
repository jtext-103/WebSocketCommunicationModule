using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Config;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

public sealed partial class WebSocketServerPhase7Tests
{
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task MinDeltaWireObservationAccumulatesForExactAndRegex(bool regex)
    {
        var saved = GlobalConfig.Get();
        try
        {
            KillHubIfAlive();
            GlobalConfig.ResourceMinDelta = 999;
            GlobalConfig.ResourceMinDeltaOverrides = new[] { new ResourceMinDeltaOverride("/device/Temperature", 10) };
            GlobalConfig.AutoObserveList = null!;
            GlobalConfig.AutoDenyList = null!;
            GlobalConfig.ResourceObservationIntervalMs = 20;
            HubMaster.InjectHubToModule(this);
            var device = new RegexDevice(); MyHub.TryAddThing(device, "/", "device");
            var listen = CreateListenUri(); using var module = StartServerModule(listen);
            using var client = await ConnectAndHelloAsync(listen);
            await SubscribeAsync(client, Guid.NewGuid(), regex ? "/device/Temp.*" : "/device/Temperature", "changed");
            Assert.IsTrue(await WaitUntilAsync(() => GetObservedResourceCount() == 1));
            var reads = device.Reads;
            Assert.IsTrue(await WaitUntilAsync(() => device.Reads >= reads + 3));
            device.Value = 5; reads = device.Reads;
            Assert.IsTrue(await WaitUntilAsync(() => device.Reads >= reads + 3));
            device.Value = 9; reads = device.Reads;
            Assert.IsTrue(await WaitUntilAsync(() => device.Reads >= reads + 3));
            device.Value = 11;
            // Any initial or below-threshold event would arrive before this value and fail the assertion.
            Assert.AreEqual(11, (await ReceiveUntilAsync<RemoteEventMessage>(client)).EventArg!.Sample.GetVal<int>());
            device.Value = 15; reads = device.Reads;
            Assert.IsTrue(await WaitUntilAsync(() => device.Reads >= reads + 3));
            device.Value = 21;
            Assert.AreEqual(21, (await ReceiveUntilAsync<RemoteEventMessage>(client)).EventArg!.Sample.GetVal<int>());
        }
        finally { KillHubIfAlive(); GlobalConfig.Set(saved); }
    }
}
