using System.Collections.Concurrent;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Newtonsoft.Json.Linq;

namespace TestCode;

[TestClass]
public sealed class WebSocketProtocolPhase2Tests
{
    [TestMethod]
    public void ProtocolConstants_ShouldUseV2EndpointAndSubProtocol()
    {
        Assert.AreEqual("/cfet2/event/v2", GetConstant(nameof(WebSocketProtocolConstants.EndpointPath)));
        Assert.AreEqual("cfet2-event.v2", GetConstant(nameof(WebSocketProtocolConstants.SubProtocol)));
        Assert.AreEqual("cfet2.websocket.event", GetConstant(nameof(WebSocketProtocolConstants.Protocol)));
        Assert.AreEqual(2, GetConstant(nameof(WebSocketProtocolConstants.Version)));
    }

    [TestMethod]
    public void SerializeToUtf8Bytes_ShouldWriteCamelCaseEnvelope()
    {
        var bytes = WebSocketMessageSerializer.SerializeToUtf8Bytes(new HelloMessage
        {
            ClientId = "client-1",
        });

        var json = JObject.Parse(System.Text.Encoding.UTF8.GetString(bytes));

        Assert.AreEqual("hello", json.Value<string>("messageType"));
        Assert.AreEqual("cfet2.websocket.event", json.Value<string>("protocol"));
        Assert.AreEqual(2, json.Value<int>("version"));
        Assert.AreEqual("client-1", json.Value<string>("clientId"));
        Assert.IsNull(json["authToken"]);
    }

    [TestMethod]
    public void DeserializeEnvelope_ShouldValidateAndReturnEnvelopeOnly()
    {
        var envelope = WebSocketMessageSerializer.DeserializeEnvelope("""
            {
              "messageType": "subscribe",
              "protocol": "cfet2.websocket.event",
              "version": 2,
              "ignored": true
            }
            """);

        Assert.AreEqual(WebSocketProtocolConstants.Subscribe, envelope.MessageType);
        Assert.AreEqual(WebSocketProtocolConstants.Protocol, envelope.Protocol);
        Assert.AreEqual(WebSocketProtocolConstants.Version, envelope.Version);
    }

    [TestMethod]
    public void DeserializeMessage_ShouldDispatchHelloAck()
    {
        var message = WebSocketMessageSerializer.DeserializeMessage("""
            {
              "messageType": "helloAck",
              "protocol": "cfet2.websocket.event",
              "version": 2,
              "accepted": true,
              "heartbeatIntervalMilliseconds": 30000,
              "heartbeatTimeoutMilliseconds": 90000
            }
            """);

        var helloAck = message as HelloAckMessage;
        Assert.IsNotNull(helloAck);
        Assert.IsTrue(helloAck.Accepted);
        Assert.AreEqual(30000, helloAck.HeartbeatIntervalMilliseconds);
        Assert.AreEqual(90000, helloAck.HeartbeatTimeoutMilliseconds);
    }

    [TestMethod]
    public void SubscribeMessage_ShouldRoundTripFilter()
    {
        var tokenId = Guid.NewGuid();
        var filter = new EventFilter("/thing/status", "Changed", 1, "ws://127.0.0.1:9000/");
        var bytes = WebSocketMessageSerializer.SerializeToUtf8Bytes(new SubscribeMessage
        {
            TokenId = tokenId,
            Filter = filter,
        });

        var message = WebSocketMessageSerializer.DeserializeMessage(bytes) as SubscribeMessage;

        Assert.IsNotNull(message);
        Assert.AreEqual(tokenId, message.TokenId);
        Assert.IsNotNull(message.Filter);
        Assert.AreEqual("ws://127.0.0.1:9000", message.Filter.Host);
        Assert.AreEqual(1, message.Filter.PerformanceLevel);
        Assert.AreEqual("/thing/status", message.Filter.SourcesAndTypes.Single().Source);
        Assert.AreEqual("Changed", message.Filter.SourcesAndTypes.Single().EventType);
    }

    [TestMethod]
    public void SubscribeMessages_ShouldRoundTripRevision()
    {
        var subscribe = (SubscribeMessage)WebSocketMessageSerializer.DeserializeMessage(
            WebSocketMessageSerializer.SerializeToUtf8Bytes(new SubscribeMessage
            {
                TokenId = Guid.NewGuid(),
                Revision = 7,
                Filter = new EventFilter("/thing/status", "Changed"),
            }));
        var ack = (SubscribeAckMessage)WebSocketMessageSerializer.DeserializeMessage(
            WebSocketMessageSerializer.SerializeToUtf8Bytes(new SubscribeAckMessage
            {
                TokenId = subscribe.TokenId,
                Revision = 7,
                Accepted = true,
            }));

        Assert.AreEqual(7, subscribe.Revision);
        Assert.AreEqual(7, ack.Revision);
    }

    [TestMethod]
    public void LightweightRemoteEvent_ShouldNotSerializeFullEventArgOrContext()
    {
        var json = WebSocketMessageSerializer.Serialize(new RemoteEventMessage
        {
            TokenId = Guid.NewGuid(),
            PerformanceLevel = 1,
            Source = "/thing/status",
            EventType = "Changed",
            Value = 42,
        });

        var message = (RemoteEventMessage)WebSocketMessageSerializer.DeserializeMessage(json);

        Assert.IsFalse(json.Contains("\"eventArg\"", StringComparison.Ordinal));
        Assert.IsFalse(json.Contains("\"context\"", StringComparison.Ordinal));
        Assert.AreEqual(1, message.EventArg!.PerformanceLevel);
        Assert.AreEqual(42L, message.EventArg.Sample.GetVal<long>());
    }

    [TestMethod]
    public void DeserializeMessage_ShouldDispatchAllMessageTypes()
    {
        Assert.IsInstanceOfType(RoundTrip(new HelloMessage()), typeof(HelloMessage));
        Assert.IsInstanceOfType(RoundTrip(new HelloAckMessage()), typeof(HelloAckMessage));
        Assert.IsInstanceOfType(RoundTrip(new SubscribeMessage()), typeof(SubscribeMessage));
        Assert.IsInstanceOfType(RoundTrip(new SubscribeAckMessage()), typeof(SubscribeAckMessage));
        Assert.IsInstanceOfType(RoundTrip(new UnsubscribeMessage()), typeof(UnsubscribeMessage));
        Assert.IsInstanceOfType(RoundTrip(new RemoteEventMessage()), typeof(RemoteEventMessage));
        Assert.IsInstanceOfType(RoundTrip(new PingMessage()), typeof(PingMessage));
        Assert.IsInstanceOfType(RoundTrip(new PongMessage()), typeof(PongMessage));
        Assert.IsInstanceOfType(RoundTrip(new ErrorMessage()), typeof(ErrorMessage));
    }

    [TestMethod]
    public void RemoteEventMessage_ShouldKeepTokenIdOptional()
    {
        var json = WebSocketMessageSerializer.Serialize(new RemoteEventMessage
        {
            EventArg = new EventArg("/thing/status", "Changed", null),
        });

        var obj = JObject.Parse(json);

        Assert.IsNull(obj["tokenId"]);
        Assert.AreEqual("event", obj.Value<string>("messageType"));
        Assert.AreEqual("/thing/status", obj["eventArg"]?.Value<string>("source"));
    }

    [TestMethod]
    public void RemoteEventMessage_ShouldIgnoreUnknownTokenIdsField()
    {
        var message = WebSocketMessageSerializer.DeserializeMessage("""
            {
              "messageType": "event",
              "protocol": "cfet2.websocket.event",
              "version": 2,
              "tokenIds": [
                "bf5e6d89-7d31-4db5-9185-1df3baf8e17a"
              ],
              "eventArg": {
                "source": "/thing/status",
                "eventType": "Changed"
              }
            }
            """) as RemoteEventMessage;

        Assert.IsNotNull(message);
        Assert.IsNull(message.TokenId);
        Assert.IsNotNull(message.EventArg);
        Assert.AreEqual("/thing/status", message.EventArg.Source);
    }

    [TestMethod]
    public void BadJson_ShouldThrowFormatException()
    {
        AssertThrowsFormatException(() =>
            WebSocketMessageSerializer.DeserializeEnvelope("{ not json"));
    }

    [TestMethod]
    public void MissingMessageType_ShouldThrowFormatException()
    {
        AssertThrowsFormatException(() =>
            WebSocketMessageSerializer.DeserializeEnvelope("""
                {
                  "protocol": "cfet2.websocket.event",
                  "version": 2
                }
                """));
    }

    [TestMethod]
    public void WrongProtocol_ShouldThrowFormatException()
    {
        AssertThrowsFormatException(() =>
            WebSocketMessageSerializer.DeserializeEnvelope("""
                {
                  "messageType": "hello",
                  "protocol": "old.protocol",
                  "version": 2
                }
                """));
    }

    [TestMethod]
    public void WrongVersion_ShouldThrowFormatException()
    {
        AssertThrowsFormatException(() =>
            WebSocketMessageSerializer.DeserializeEnvelope("""
                {
                  "messageType": "hello",
                  "protocol": "cfet2.websocket.event",
                  "version": 1
                }
                """));
    }

    [TestMethod]
    public void UnknownMessageType_ShouldThrowWhenDispatching()
    {
        AssertThrowsFormatException(() =>
            WebSocketMessageSerializer.DeserializeMessage("""
                {
                  "messageType": "legacySubscribe",
                  "protocol": "cfet2.websocket.event",
                  "version": 2
                }
                """));
    }

    [TestMethod]
    public void OversizedMessage_ShouldThrowFormatException()
    {
        var bytes = WebSocketMessageSerializer.SerializeToUtf8Bytes(new HelloMessage());

        AssertThrowsFormatException(() =>
            WebSocketMessageSerializer.DeserializeEnvelope(bytes, maxMessageBytes: bytes.Length - 1));
    }

    [TestMethod]
    public async Task Serializer_ShouldSupportHighConcurrencyRoundTrips()
    {
        var failures = new ConcurrentQueue<Exception>();

        await Parallel.ForEachAsync(
            Enumerable.Range(0, 10_000),
            new ParallelOptions { MaxDegreeOfParallelism = Math.Max(4, Environment.ProcessorCount * 2) },
            (index, _) =>
            {
                try
                {
                    var message = RoundTrip(new RemoteEventMessage
                    {
                        TokenId = Guid.NewGuid(),
                        EventArg = new EventArg(
                            $"/thing/{index}",
                            "Changed",
                            new Jtext103.CFET2.Core.Sample.Status<int>(index)),
                    });

                    var remoteEvent = (RemoteEventMessage)message;
                    Assert.AreEqual($"/thing/{index}", remoteEvent.EventArg!.Source);
                    Assert.AreEqual(index, remoteEvent.EventArg.Sample.GetVal<int>());
                }
                catch (Exception ex)
                {
                    failures.Enqueue(ex);
                }

                return ValueTask.CompletedTask;
            });

        Assert.IsEmpty(failures, string.Join(Environment.NewLine, failures.Take(3).Select(error => error.ToString())));
    }

    private static object? GetConstant(string name)
    {
        return typeof(WebSocketProtocolConstants).GetField(name)?.GetValue(null);
    }

    private static void AssertThrowsFormatException(Action action)
    {
        try
        {
            action();
        }
        catch (FormatException)
        {
            return;
        }

        Assert.Fail("Expected FormatException.");
    }

    private static WebSocketMessageEnvelope RoundTrip(WebSocketMessageEnvelope message)
    {
        return WebSocketMessageSerializer.DeserializeMessage(
            WebSocketMessageSerializer.SerializeToUtf8Bytes(message));
    }
}
