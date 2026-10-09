using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Newtonsoft.Json.Linq;

namespace TestCode;

[TestClass]
public sealed class WebSocketTimestampTests
{
    private static readonly DateTimeOffset SourceTime = new DateTimeOffset(2026, 9, 24, 12, 0, 0, TimeSpan.FromHours(8)).AddTicks(1234567);

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    public void EventTimestamp_ShouldRoundTripAtBothPerformanceLevels(int level)
    {
        var message = new RemoteEventMessage
        {
            PerformanceLevel = level,
            Source = "/value",
            EventType = "changed",
            Value = level == 1 ? 42 : null,
            Timestamp = level == 1 ? SourceTime : null,
            EventArg = level == 0 ? new EventArg("/value", "changed", new Status<int>(42) { Timestamp = SourceTime }) : null,
        };
        var received = (RemoteEventMessage)WebSocketMessageSerializer.DeserializeMessage(WebSocketMessageSerializer.Serialize(message));
        Assert.AreEqual(SourceTime, received.EventArg!.Sample.Timestamp);
        Assert.AreEqual(TimeSpan.Zero, received.EventArg.Sample.Timestamp!.Value.Offset);
        Assert.AreEqual(42, received.EventArg.Sample.GetVal<int>());
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    public void LegacyOrMalformedTimestamp_ShouldRemainUnknown(int level)
    {
        foreach (var timestamp in new JToken?[] { null, new JValue("not-a-date"), new JValue(123), new JObject() })
        {
            var obj = JObject.Parse(WebSocketMessageSerializer.Serialize(new RemoteEventMessage
            {
                PerformanceLevel = level,
                Value = 42,
                EventArg = level == 0 ? new EventArg("/value", "changed", new Status<int>(42)) : null,
            }));
            if (timestamp != null)
            {
                if (level == 1) obj["timestamp"] = timestamp;
                else obj["eventArg"]!["sample"]!["context"]![SampleBase<int>.KeyOfTimestamp] = timestamp;
            }
            var received = (RemoteEventMessage)WebSocketMessageSerializer.DeserializeMessage(obj.ToString());
            Assert.IsNull(received.EventArg!.Sample.Timestamp);
        }
    }

    [TestMethod]
    public void ContextlessSample_ShouldReadTimestamp()
    {
        var obj = JObject.Parse(WebSocketMessageSerializer.Serialize(new RemoteEventMessage
        {
            EventArg = new EventArg("/value", "changed", new Status<int>(42) { Timestamp = SourceTime }),
        }));
        ((JObject)obj["eventArg"]!["sample"]!).Remove("context");
        var received = (RemoteEventMessage)WebSocketMessageSerializer.DeserializeMessage(obj.ToString());
        Assert.AreEqual(SourceTime, received.EventArg!.Sample.Timestamp);
    }
}
