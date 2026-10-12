using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Attributes;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

public sealed partial class WebSocketServerPhase7Tests
{
    [TestMethod]
    [DataRow(0, false)]
    [DataRow(0, true)]
    [DataRow(1, false)]
    [DataRow(1, true)]
    public async Task ActiveChanges_ParameterizedExactAndRegexTransmitValuesAndTimestamp(int performanceLevel, bool regex)
    {
        var thing = new ActiveChangeThing();
        MyHub.TryAddThing(thing, "/", "active");
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        var token = Guid.NewGuid();
        await SubscribeAsync(client, token, regex ? "/active/Temperature/.*" : "/active/Temperature/1", "changed", performanceLevel: performanceLevel);
        Assert.AreEqual(0, GetObservedResourceCount());
        var sourceTime = new DateTimeOffset(2026, 10, 10, 2, 0, 0, TimeSpan.Zero);
        Assert.IsTrue(thing.PublishResourceIfChanged("Temperature", new[] { 10, 20 }, new object[] { 1 }, sourceTime));
        Assert.IsFalse(thing.PublishResourceIfChanged("Temperature", new[] { 10, 20 }, new object[] { 1 }, sourceTime.AddMinutes(1)));
        var message = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(token, message.TokenId);
        Assert.AreEqual("/active/Temperature/1", message.EventArg!.Source);
        CollectionAssert.AreEqual(new[] { 10, 20 }, message.EventArg.Sample.GetVal<int[]>());
        Assert.AreEqual(sourceTime, message.EventArg.Sample.Timestamp);
        Assert.AreEqual(0, thing.Reads);
    }

    private sealed class ActiveChangeThing : Thing
    {
        public int Reads;
        [Cfet2Status, Cfet2Event] public int[] Temperature(int channel) { Reads++; return new[] { channel }; }
    }

    [TestMethod]
    [DataRow(0, 0)]
    [DataRow(0, 1)]
    [DataRow(1, 0)]
    [DataRow(1, 1)]
    [DataRow(2, 0)]
    [DataRow(2, 1)]
    public async Task Containers_ExactAndRegexShouldObserveAndTransmitWithSourceTimestamp(int kind, int performanceLevel)
    {
        var thing = new ContainerObservedThing();
        MyHub.TryAddThing(thing, "/", "containers");
        var listenUri = CreateListenUri();
        using var module = StartServerModule(listenUri);
        using var client = await ConnectAndHelloAsync(listenUri);
        string member = new[] { "Array", "List", "Dictionary" }[kind];
        var tokenId = Guid.NewGuid();
        await SubscribeAsync(client, tokenId, performanceLevel == 0 ? "/containers/" + member : "/containers/.*", "changed", performanceLevel: performanceLevel);
        Assert.AreEqual(performanceLevel == 0 ? 1 : 3, GetObservedResourceCount());
        switch (kind)
        {
            case 0: thing.Array.Val[0] = 2; break;
            case 1: thing.List.Val[0] = 2; break;
            case 2: thing.Dictionary.Val["a"][0] = 2; break;
        }
        var message = await ReceiveUntilAsync<RemoteEventMessage>(client);
        Assert.AreEqual(tokenId, message.TokenId);
        Assert.AreEqual("/containers/" + member, message.EventArg!.Source);
        Assert.AreEqual(ContainerObservedThing.SourceTime, message.EventArg.Sample.Timestamp);
        switch (kind)
        {
            case 0: CollectionAssert.AreEqual(new[] { 2 }, message.EventArg.Sample.GetVal<int[]>()); break;
            case 1: CollectionAssert.AreEqual(new List<int> { 2 }, message.EventArg.Sample.GetVal<List<int>>()); break;
            case 2: CollectionAssert.AreEqual(new[] { 2 }, message.EventArg.Sample.GetVal<Dictionary<string, int[]>>()["a"]); break;
        }
    }

    private sealed class ContainerObservedThing : Thing
    {
        public static readonly DateTimeOffset SourceTime = new(2026, 10, 10, 0, 0, 0, TimeSpan.Zero);
        [Cfet2Status] public Status<int[]> Array { get; } = new(new[] { 1 }) { Timestamp = SourceTime };
        [Cfet2Status] public Status<List<int>> List { get; } = new(new List<int> { 1 }) { Timestamp = SourceTime };
        [Cfet2Status] public Status<Dictionary<string, int[]>> Dictionary { get; } = new(new Dictionary<string, int[]> { ["a"] = new[] { 1 } }) { Timestamp = SourceTime };
    }
}
