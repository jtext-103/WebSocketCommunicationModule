using System.Collections.Concurrent;
using System.Numerics;
using Jtext103.CFET2.Core.Sample;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace TestCode;

[TestClass]
public sealed class WebSocketContextAllocationTests
{
    private static readonly JsonSerializerSettings LegacySettings = new()
    {
        ContractResolver = new CamelCasePropertyNamesContractResolver(),
        NullValueHandling = NullValueHandling.Ignore,
    };

    [TestMethod]
    [DataRow("{}")]
    [DataRow("{\"nothing\":null,\"flag\":true,\"integer\":42,\"negative\":-12,\"text\":\"hello\"}")]
    [DataRow("{\"max\":9223372036854775807,\"big\":1844674407370955161612345,\"fraction\":1.2345678901234567,\"exp\":1e100}")]
    [DataRow("{\"date\":\"2026-10-19T12:34:56.1234567+08:00\",\"badDate\":\"not-a-date\",\"guid\":\"a671ea3e-bf86-4ca5-a78f-721a8b8ab953\"}")]
    [DataRow("{\"object\":{\"n\":123,\"nested\":[null,true,{\"x\":\"中🙂\"}]},\"array\":[1,\"x\",null,[2,3]]}")]
    [DataRow("{\"设备\":\"中\",\"é\":1,\"Straße\":2,\"κ\":3}")]
    [DataRow("{\"same\":1,\"same\":2}")]
    [DataRow("{\"unknown\":undefined,\"nan\":NaN,\"infinity\":Infinity}")]
    [DataRow("{\"$id\":\"context-1\",\"value\":1}")]
    [DataRow("{\"value\":1,\"$id\":\"context-1\"}")]
    [DataRow("{\"$id\":null,\"value\":1}")]
    [DataRow("{\"$type\":\"Ignored.Type\",\"value\":1}")]
    [DataRow("{\"$ref\":null,\"value\":1}")]
    [DataRow("{\"$ref\":\"missing\"}")]
    [DataRow("{\"$value\":17,\"value\":1}")]
    [DataRow("{\"$custom\":1,\"$id\":\"ordinary-later-key\"}")]
    [DataRow("{\"$ID\":\"ordinary\",\"value\":1}")]
    [DataRow("{\"payload\":{\"$id\":\"nested\",\"value\":1},\"items\":[{\"$type\":\"Ignored.Type\",\"value\":2},{\"$ref\":\"missing\"}]}")]
    public void Context_ShouldMatchLegacyDictionaryConversion(string contextJson)
    {
        // The expected graph deliberately uses the frozen pre-P3 conversion algorithm.
        var raw = JObject.Parse(contextJson).ToObject<Dictionary<string, object>>(JsonSerializer.Create(LegacySettings))
            ?? new Dictionary<string, object>();
        var expected = new Dictionary<string, object>(raw, StringComparer.InvariantCultureIgnoreCase);
        var actual = ReadContext(contextJson);
        Assert.AreSame(StringComparer.InvariantCultureIgnoreCase, actual.Comparer);
        CollectionAssert.AreEqual(expected.Keys.ToArray(), actual.Keys.ToArray());
        foreach (var key in expected.Keys) AssertSameValue(expected[key], actual[key]);
    }

    [TestMethod]
    [DataRow("{\"$ref\":123}")]
    [DataRow("{\"$ref\":\"missing\",\"value\":1}")]
    [DataRow("{\"$values\":[]}")]
    [DataRow("{\"$id\":\"context-1\",\"Name\":1,\"name\":2}")]
    [DataRow("{\"$ID\":\"one\",\"$id\":\"two\"}")]
    public void InvalidMetadata_ShouldKeepLegacyExceptionClassification(string contextJson)
    {
        Exception? legacy = null;
        try
        {
            var raw = JObject.Parse(contextJson).ToObject<Dictionary<string, object>>(JsonSerializer.Create(LegacySettings))
                ?? new Dictionary<string, object>();
            _ = new Dictionary<string, object>(raw, StringComparer.InvariantCultureIgnoreCase);
        }
        catch (Exception exception) { legacy = exception; }
        Assert.IsNotNull(legacy, "The fixture must exercise an existing failure.");
        var actual = Assert.ThrowsExactly<FormatException>(() => ReadContext(contextJson));
        Assert.IsNotNull(actual.InnerException);
        Assert.AreEqual(legacy.GetType(), actual.InnerException.GetType());
    }

    [TestMethod]
    public void ContextReference_ShouldKeepLegacyGraphAndCopyOwnership()
    {
        var context = ReadContext("{\"$id\":\"context-1\",\"self\":{\"$ref\":\"context-1\"},\"value\":1}");
        Assert.IsFalse(context.ContainsKey("$id"));
        Assert.AreEqual(1L, context["value"]);
        Assert.IsInstanceOfType<Dictionary<string, object>>(context["self"]);
        var referenced = (Dictionary<string, object>)context["self"];
        Assert.AreNotSame(context, referenced, "Legacy copy leaves references pointing at the original decoded dictionary.");
        Assert.AreSame(referenced, referenced["self"]);
        Assert.AreEqual(1L, referenced["value"]);
    }

    [TestMethod]
    [DataRow("Name", "name")]
    [DataRow("é", "e\u0301")]
    [DataRow("I", "ı")]
    [DataRow("a\0b", "ab")]
    public void CultureEquivalentKeys_ShouldPreserveConflictFailure(string first, string second)
    {
        var json = new JObject { [first] = 1, [second] = 2 }.ToString(Formatting.None);
        if (StringComparer.InvariantCultureIgnoreCase.Equals(first, second))
        {
            var exception = Assert.ThrowsExactly<FormatException>(() => ReadContext(json));
            Assert.IsInstanceOfType<ArgumentException>(exception.InnerException);
        }
        else
        {
            var context = ReadContext(json);
            Assert.AreEqual(2, context.Count);
            Assert.AreEqual(1L, context[first]);
            Assert.AreEqual(2L, context[second]);
        }
    }

    [TestMethod]
    public void Metadata_ShouldPreserveOriginalKeysAndValuesWithoutStatusDefaults()
    {
        var context = new JObject {
            [SampleBase<object>.KeyOfVal] = new JObject { ["seq"] = 17 },
            [SampleBase<object>.KeyOfIsValid.ToLowerInvariant()] = false,
            [SampleBase<object>.KeyOfErrorMessage] = new JArray("error", "错误"),
            [SampleBase<object>.KeyOfPath] = "/remote/value",
            [SampleBase<object>.KeyOfCchildThing] = "/remote",
            [SampleBase<object>.KeyOfParametersList] = new JArray(1, "two"),
            [SampleBase<object>.KeyOfResourceType] = 123,
            [SampleBase<object>.KeyOfIsRemote] = true,
            [SampleBase<object>.KeyOfTimestamp] = "2026-10-19T12:34:56.1234567+08:00",
            ["extension"] = JValue.CreateNull(),
        };
        var actual = ReadContext(context.ToString(Formatting.None));
        Assert.AreEqual(context.Count, actual.Count);
        Assert.AreEqual(false, actual[SampleBase<object>.KeyOfIsValid]);
        Assert.AreEqual(123L, actual[SampleBase<object>.KeyOfResourceType]);
        Assert.IsTrue(actual.Keys.Contains(SampleBase<object>.KeyOfIsValid.ToLowerInvariant(), StringComparer.Ordinal));
        Assert.AreEqual("/remote/value", actual[SampleBase<object>.KeyOfPath]);
        Assert.IsNull(actual["extension"]);
        Context_ShouldMatchLegacyDictionaryConversion(context.ToString(Formatting.None));
    }

    [TestMethod]
    [DataRow("{\"objectVal\":17}")]
    [DataRow("{\"val\":17}")]
    [DataRow("{\"context\":null,\"objectVal\":17}")]
    [DataRow("{\"context\":[],\"val\":17}")]
    [DataRow("17")]
    public void LegacySamplesWithoutObjectContext_ShouldKeepFallback(string sample)
    {
        var message = ReadSample(sample);
        Assert.IsNotNull(message.EventArg!.Sample);
        Assert.AreEqual(17, message.EventArg.Sample.GetVal<int>());
    }

    [TestMethod]
    public void NullSample_ShouldRemainNull()
    {
        Assert.IsNull(ReadSample("null").EventArg!.Sample);
    }

    [TestMethod]
    public void LargeInteger_ShouldKeepBigIntegerPrecision()
    {
        const string digits = "1844674407370955161612345";
        var context = ReadContext("{\"value\":" + digits + "}");
        Assert.IsInstanceOfType<BigInteger>(context["value"]);
        Assert.AreEqual(BigInteger.Parse(digits), context["value"]);
    }

    [TestMethod]
    public void ConcurrentMessages_ShouldKeepIndependentRetainedContextsAndNestedObjects()
    {
        var retained = new ConcurrentDictionary<int, Dictionary<string, object>>();
        Parallel.For(0, 256, new ParallelOptions { MaxDegreeOfParallelism = 8 }, sequence => {
            var context = ReadContext("{\"seq\":" + sequence + ",\"payload\":{\"seq\":" + sequence + "}}");
            Assert.IsTrue(retained.TryAdd(sequence, context));
        });
        foreach (var pair in retained)
        {
            Assert.AreEqual((long)pair.Key, pair.Value["seq"]);
            Assert.AreEqual(pair.Key, ((JObject)pair.Value["payload"]).Value<int>("seq"));
        }
        Assert.AreNotSame(retained[0], retained[1]);
        Assert.AreNotSame(retained[0]["payload"], retained[1]["payload"]);
        ((JObject)retained[0]["payload"])["seq"] = -1;
        Assert.AreEqual(1, ((JObject)retained[1]["payload"]).Value<int>("seq"));
    }

    private static Dictionary<string, object> ReadContext(string context) =>
        ReadSample("{\"context\":" + context + "}").EventArg!.Sample!.Context;

    private static RemoteEventMessage ReadSample(string sample) => (RemoteEventMessage)WebSocketMessageSerializer.DeserializeMessage(
        "{\"messageType\":\"event\",\"protocol\":\"cfet2.websocket.event\",\"version\":2,\"eventArg\":{\"performanceLevel\":0,\"host\":\"remote\",\"source\":\"/value\",\"eventType\":\"changed\",\"sample\":" + sample + "}}");

    private static void AssertSameValue(object? expected, object? actual)
    {
        if (expected == null) { Assert.IsNull(actual); return; }
        Assert.IsNotNull(actual);
        Assert.AreEqual(expected.GetType(), actual.GetType(), "JSON runtime value type changed.");
        if (expected is JToken token)
            Assert.IsTrue(JToken.DeepEquals(token, (JToken)actual), "Nested JSON graph changed.");
        else if (expected is DateTime time)
        {
            Assert.AreEqual(time.Ticks, ((DateTime)actual).Ticks);
            Assert.AreEqual(time.Kind, ((DateTime)actual).Kind);
        }
        else Assert.AreEqual(expected, actual);
    }
}
