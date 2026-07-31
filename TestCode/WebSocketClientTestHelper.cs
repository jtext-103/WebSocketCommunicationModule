using System.Collections.Concurrent;
using System.Reflection;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Client;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;

namespace TestCode;

internal static class WebSocketClientTestHelper
{
    public static List<RemoteEndpointClient> GetEndpointClients(WebSocketCommunicationModule module)
    {
        var field = typeof(WebSocketCommunicationModule).GetField("endpointClients", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(field);
        var dict = (ConcurrentDictionary<RemoteEndpointKey, RemoteEndpointClient>)field.GetValue(module)!;
        return dict.Values.ToList();
    }

    public static RemoteEndpointClient GetOnlyEndpointClient(WebSocketCommunicationModule module)
    {
        return GetEndpointClients(module).Single();
    }

    public static RemoteSubscriptionEntry? GetSubscriptionForLocalToken(WebSocketCommunicationModule module, Guid localTokenId)
    {
        return GetMasterTokenIds(module, localTokenId)
            .Select(masterTokenId => GetEndpointClients(module)
                .Select(client => client.GetSubscription(masterTokenId))
                .FirstOrDefault(subscription => subscription != null))
            .FirstOrDefault(subscription => subscription != null);
    }

    public static Guid[] GetMasterTokenIds(WebSocketCommunicationModule module, Guid localTokenId)
    {
        var aggregator = GetAggregator(module);
        var field = aggregator.GetType().GetField("localSubscriptionIndex", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(field);
        var dict = field.GetValue(aggregator)!;
        var args = new object?[] { localTokenId, null };
        var found = (bool)dict.GetType().GetMethod("TryGetValue")!.Invoke(dict, args)!;
        if (!found)
        {
            return [];
        }

        return (Guid[])args[1]!.GetType().GetProperty("MasterTokenIds")!.GetValue(args[1])!;
    }

    public static int GetDictionaryCount(WebSocketCommunicationModule module, string fieldName)
    {
        if (fieldName == "subscriptions")
        {
            return GetEndpointClients(module).Sum(client => client.SubscriptionCount);
        }

        if (fieldName == "connections")
        {
            return GetEndpointClients(module).Count;
        }

        var target = (object)module;
        var field = typeof(WebSocketCommunicationModule).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        if (field == null)
        {
            target = GetAggregator(module);
            field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        }

        Assert.IsNotNull(field);
        var value = field.GetValue(target)!;
        return (int)value.GetType().GetProperty("Count")!.GetValue(value)!;
    }

    private static object GetAggregator(WebSocketCommunicationModule module)
    {
        var aggregatorField = typeof(WebSocketCommunicationModule).GetField(
            "subscriptionAggregator",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(aggregatorField);
        return aggregatorField.GetValue(module)!;
    }
}
