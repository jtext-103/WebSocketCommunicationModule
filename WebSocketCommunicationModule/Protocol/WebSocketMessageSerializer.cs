using System;
using System.Collections.Generic;
using System.Text;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Sample;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    public static class WebSocketMessageSerializer
    {
        private static readonly JsonSerializerSettings JsonSettings = new()
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            NullValueHandling = NullValueHandling.Ignore,
        };

        private static readonly JsonSerializer JsonSerializer = JsonSerializer.Create(JsonSettings);

        public static byte[] SerializeToUtf8Bytes(WebSocketMessageEnvelope message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message, JsonSettings));
        }

        public static string Serialize(WebSocketMessageEnvelope message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            return JsonConvert.SerializeObject(message, JsonSettings);
        }

        public static WebSocketMessageEnvelope DeserializeEnvelope(byte[] utf8Bytes, int maxMessageBytes = int.MaxValue)
        {
            if (utf8Bytes == null)
            {
                throw new ArgumentNullException(nameof(utf8Bytes));
            }

            if (utf8Bytes.Length > maxMessageBytes)
            {
                throw new FormatException("WebSocket message exceeds the maximum allowed size.");
            }

            return DeserializeEnvelope(Encoding.UTF8.GetString(utf8Bytes));
        }

        public static WebSocketMessageEnvelope DeserializeEnvelope(string json)
        {
            var obj = ParseAndValidateEnvelope(json);
            return new RawEnvelope(
                obj.Value<string>("messageType")!,
                obj.Value<string>("protocol")!,
                obj.Value<int>("version"));
        }

        public static WebSocketMessageEnvelope DeserializeMessage(byte[] utf8Bytes, int maxMessageBytes = int.MaxValue)
        {
            if (utf8Bytes == null)
            {
                throw new ArgumentNullException(nameof(utf8Bytes));
            }

            if (utf8Bytes.Length > maxMessageBytes)
            {
                throw new FormatException("WebSocket message exceeds the maximum allowed size.");
            }

            return DeserializeMessage(Encoding.UTF8.GetString(utf8Bytes));
        }

        public static WebSocketMessageEnvelope DeserializeMessage(string json)
        {
            try
            {
                return DeserializeMessageCore(json);
            }
            catch (Exception ex) when (ex is JsonException or InvalidCastException or OverflowException or ArgumentException)
            {
                throw new FormatException("WebSocket message contains invalid fields.", ex);
            }
        }

        private static WebSocketMessageEnvelope DeserializeMessageCore(string json)
        {
            var obj = ParseAndValidateEnvelope(json);
            var messageType = obj.Value<string>("messageType")!;

            return messageType switch
            {
                WebSocketProtocolConstants.Hello => ToMessage<HelloMessage>(obj),
                WebSocketProtocolConstants.HelloAck => ToMessage<HelloAckMessage>(obj),
                WebSocketProtocolConstants.Subscribe => ToMessage<SubscribeMessage>(obj),
                WebSocketProtocolConstants.SubscribeAck => ToMessage<SubscribeAckMessage>(obj),
                WebSocketProtocolConstants.Unsubscribe => ToMessage<UnsubscribeMessage>(obj),
                WebSocketProtocolConstants.Event => ToRemoteEventMessage(obj),
                WebSocketProtocolConstants.Ping => ToMessage<PingMessage>(obj),
                WebSocketProtocolConstants.Pong => ToMessage<PongMessage>(obj),
                WebSocketProtocolConstants.Heartbeat => ToMessage<HeartbeatMessage>(obj),
                WebSocketProtocolConstants.Error => ToMessage<ErrorMessage>(obj),
                _ => throw new FormatException($"Unsupported WebSocket message type: {messageType}."),
            };
        }

        private static T ToMessage<T>(JObject obj)
            where T : WebSocketMessageEnvelope
        {
            return obj.ToObject<T>(JsonSerializer)
                ?? throw new FormatException($"Cannot deserialize WebSocket message as {typeof(T).Name}.");
        }

        private static RemoteEventMessage ToRemoteEventMessage(JObject obj)
        {
            var eventArgToken = obj["eventArg"];
            if (eventArgToken == null || eventArgToken.Type == JTokenType.Null)
            {
                if (obj.Value<int?>("performanceLevel") == 1)
                {
                    var valueToken = obj["value"];
                    var timestamp = ReadTimestamp(obj["timestamp"]);
                    return new RemoteEventMessage
                    {
                        TokenId = ReadNullableGuid(obj["tokenId"]),
                        PerformanceLevel = 1,
                        Source = obj.Value<string>("source") ?? string.Empty,
                        EventType = obj.Value<string>("eventType") ?? string.Empty,
                        Value = valueToken?.ToObject<object>(JsonSerializer),
                        Timestamp = timestamp,
                        EventArg = new EventArg
                        {
                            PerformanceLevel = 1,
                            Source = obj.Value<string>("source") ?? string.Empty,
                            EventType = obj.Value<string>("eventType") ?? string.Empty,
                            Sample = new Status<object>(valueToken?.ToObject<object>(JsonSerializer)!)
                            {
                                Timestamp = timestamp,
                            },
                        },
                    };
                }

                return ToMessage<RemoteEventMessage>(obj);
            }

            if (eventArgToken is not JObject eventArgObj)
            {
                throw new FormatException("Remote event message eventArg must be an object.");
            }

            var sample = DeserializeSample(eventArgObj["sample"]);
            var eventArg = new EventArg
            {
                PerformanceLevel = eventArgObj.Value<int?>("performanceLevel") ?? 0,
                Host = eventArgObj.Value<string>("host") ?? string.Empty,
                Source = eventArgObj.Value<string>("source") ?? string.Empty,
                EventType = eventArgObj.Value<string>("eventType") ?? string.Empty,
                Sample = sample,
            };

            return new RemoteEventMessage
            {
                TokenId = ReadNullableGuid(obj["tokenId"]),
                PerformanceLevel = eventArg.PerformanceLevel,
                EventArg = eventArg,
            };
        }

        private static Guid? ReadNullableGuid(JToken? token)
        {
            if (token == null || token.Type == JTokenType.Null)
            {
                return null;
            }

            if (token.Type == JTokenType.Guid)
            {
                return token.Value<Guid>();
            }

            return Guid.TryParse(token.Value<string>(), out var guid) ? guid : null;
        }

        private static ISample? DeserializeSample(JToken? sampleToken)
        {
            if (sampleToken == null || sampleToken.Type == JTokenType.Null)
            {
                return null;
            }

            if (sampleToken is JObject sampleObj)
            {
                var contextToken = sampleObj["context"];
                if (contextToken is JObject contextObject)
                {
                    return new Status<object> { Context = DeserializeContext(contextObject) };
                }

                var objectValueToken = sampleObj["objectVal"] ?? sampleObj["val"];
                if (objectValueToken != null)
                {
                    return new Status<object>(objectValueToken.ToObject<object>(JsonSerializer)!)
                    {
                        Timestamp = ReadTimestamp(sampleObj["timestamp"]),
                    };
                }
            }

            return new Status<object>(sampleToken.ToObject<object>(JsonSerializer)!);
        }

        private static Dictionary<string, object> DeserializeContext(JObject contextObject)
        {
            // Keep Json.NET's complete metadata and reference scope, including nested values.
            if (HasLeadingContextMetadata(contextObject))
            {
                var rawContext = contextObject.ToObject<Dictionary<string, object>>(JsonSerializer)
                    ?? new Dictionary<string, object>();
                return new Dictionary<string, object>(rawContext, StringComparer.InvariantCultureIgnoreCase);
            }

            var context = new Dictionary<string, object>(contextObject.Count, StringComparer.InvariantCultureIgnoreCase);
            using (var reader = contextObject.CreateReader())
                JsonSerializer.Populate(reader, context);

            // Populate uses the dictionary indexer. Detect culture-equivalent keys that it
            // merged, then reproduce Dictionary.Add's previous exception on the error path.
            if (context.Count != contextObject.Count)
            {
                context.Clear();
                foreach (var property in contextObject.Properties())
                    context.Add(property.Name, null!);
            }
            return context;
        }

        private static bool HasLeadingContextMetadata(JObject contextObject)
        {
            // With the current default MetadataPropertyHandling, only the leading
            // properties receive special treatment. Populate keeps one reader scope
            // for nested values, so those need no separate scan or conversion.
            return contextObject.First is JProperty
            {
                Name: "$id" or "$ref" or "$type" or "$values" or "$value"
            };
        }

        private static DateTimeOffset? ReadTimestamp(JToken? token)
        {
            // Reuse the sample policy, including legacy/malformed values remaining unknown.
            var sample = new SampleBase<object>();
            if (token is JValue value && value.Value != null)
                sample.Context[SampleBase<object>.KeyOfTimestamp] = value.Value;
            return sample.Timestamp;
        }

        private static JObject ParseAndValidateEnvelope(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
            {
                throw new FormatException("WebSocket message is empty.");
            }

            JObject obj;
            try
            {
                obj = JObject.Parse(json);
            }
            catch (JsonException ex)
            {
                throw new FormatException("WebSocket message is not valid JSON.", ex);
            }

            var messageType = obj.Value<string>("messageType");
            if (string.IsNullOrWhiteSpace(messageType))
            {
                throw new FormatException("WebSocket message is missing messageType.");
            }

            var protocol = obj.Value<string>("protocol");
            if (!string.Equals(protocol, WebSocketProtocolConstants.Protocol, StringComparison.Ordinal))
            {
                throw new FormatException("WebSocket message protocol is not supported.");
            }

            var versionToken = obj["version"];
            if (versionToken == null || versionToken.Type != JTokenType.Integer)
            {
                throw new FormatException("WebSocket message is missing version.");
            }

            var version = versionToken.Value<int>();
            if (version != WebSocketProtocolConstants.Version)
            {
                throw new FormatException("WebSocket message version is not supported.");
            }

            return obj;
        }

        private sealed class RawEnvelope : WebSocketMessageEnvelope
        {
            public RawEnvelope(string messageType, string protocol, int version)
                : base(messageType)
            {
                Protocol = protocol;
                Version = version;
            }
        }
    }
}
