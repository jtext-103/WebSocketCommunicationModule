using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Attributes;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

public sealed partial class WebSocketServerPhase7Tests
{
    [TestMethod]
    [DataRow(0, 0)]
    [DataRow(0, 1)]
    [DataRow(1, 0)]
    [DataRow(1, 1)]
    [DataRow(2, 0)]
    [DataRow(2, 1)]
    [DataRow(3, 0)]
    [DataRow(3, 1)]
    [DataRow(4, 0)]
    [DataRow(4, 1)]
    [DataRow(5, 0)]
    [DataRow(5, 1)]
    public async Task ParameterizedSubscriptions_BaseDetailAllAndPrefixNeverReadOrObserve(int kind, int level)
    {
        var thing = new ParameterSubscriptionThing();
        MyHub.TryAddThing(thing, "/", "parameters");
        var listen = CreateListenUri();
        using var module = StartServerModule(listen);
        using var client = await ConnectAndHelloAsync(listen);
        var token = Guid.NewGuid();
        string filter = new[] { "/parameters/Required", "/parameters/Default", "/parameters/Variable",
            "/parameters/Multiple/1/2", "/parameters/Multiple/1/.*", "/parameters/Multiple/.*" }[kind];
        await SubscribeAsync(client, token, filter, "changed", performanceLevel: level);
        Assert.AreEqual(0, thing.Reads);
        Assert.AreEqual(0, GetObservedResourceCount());
        string source = kind < 3 ? filter : "/parameters/Multiple/1/2";
        var timestamp = new DateTimeOffset(2026, 10, 12, 1, 0, 0, TimeSpan.Zero);
        MyHub.PublishResource(source, new Status<int>(17) { Timestamp = timestamp });
        var message = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(token, message.TokenId);
        Assert.AreEqual(source, message.EventArg!.Source);
        Assert.AreEqual(timestamp, message.EventArg.Sample.Timestamp);
        Assert.AreEqual(17, message.EventArg.Sample.GetVal<int>());
        Assert.AreEqual(0, thing.Reads);
        Assert.AreEqual(0, GetObservedResourceCount());
    }

    private sealed class ParameterSubscriptionThing : Thing
    {
        public int Reads;
        [Cfet2Status] public int Required(int channel) { Reads++; return channel; }
        [Cfet2Status] public int Default(int channel = 7) { Reads++; return channel; }
        [Cfet2Status] public int Variable(params int[] channels) { Reads++; return channels.Length; }
        [Cfet2Status] public int Multiple(int device, int channel) { Reads++; return device + channel; }
    }
}
