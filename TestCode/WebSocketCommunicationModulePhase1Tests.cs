using System.Reflection;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;

namespace TestCode;

[TestClass]
[DoNotParallelize]
public sealed class WebSocketCommunicationModulePhase1Tests : CFET2Host
{
    [TestCleanup]
    public void Cleanup()
    {
        KillHubIfAlive();
    }

    [TestMethod]
    public void ProtocolNames_ShouldRegisterWsAndHttpEntrypoints()
    {
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());

        CollectionAssert.AreEquivalent(new[] { "ws", "http" }, module.ProtocolNames);
    }

    [TestMethod]
    public void GetFullHost_ForWs_ShouldReturnHostIdentity()
    {
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());

        var host = module.GetFullHost("ws", "Example.COM", 9000);

        Assert.AreEqual("ws://example.com:9000", host);
    }

    [TestMethod]
    public void GetFullHost_ForHttp_ShouldMapToAdjacentWebSocketEventHost()
    {
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());

        var host = module.GetFullHost("http", "127.0.0.1", 8080);

        Assert.AreEqual("ws://127.0.0.1:8081", host);
    }

    [TestMethod]
    public void GetFullHost_ForUnsupportedProtocol_ShouldReturnEmpty()
    {
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());

        var host = module.GetFullHost("ftp", "127.0.0.1", 8080);

        Assert.AreEqual(string.Empty, host);
    }

    [TestMethod]
    public void EventFilter_WithHttpUri_ShouldResolveHostThroughRegisteredModule()
    {
        KillHubIfAlive();
        HubMaster.InjectHubToModule(this);
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());

        MyHub.TryAddCommunicationModule(module);
        var filter = new EventFilter("http://Example.COM:8080/thing/status?x=1", "Changed");

        Assert.AreEqual("ws://example.com:8081", filter.Host);
        Assert.AreEqual("/thing/status?x=1", filter.SourcesAndTypes.Single().Source);
    }

    [TestMethod]
    public void Start_ShouldBeIdempotentWhenServerDisabled()
    {
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());

        module.Start();
        module.Start();
    }

    [TestMethod]
    public void StartCommunication_ShouldRegisterCallbackAndStartModuleThroughCore()
    {
        KillHubIfAlive();
        HubMaster.InjectHubToModule(this);
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());

        MyHub.TryAddCommunicationModule(module);

        MyHub.StartCommunication();
        MyHub.StartCommunication();
    }

    [TestMethod]
    public void RegisterEventCallback_ShouldOnlyStoreCallback()
    {
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());
        var callCount = 0;

        module.RegisterEventCallback(_ => callCount++);

        Assert.AreEqual(0, callCount);
    }

    [TestMethod]
    public void RaiseConnectionStatus_ShouldInvokeRegisteredCallback()
    {
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());
        EventArg? received = null;
        module.RegisterEventCallback(arg => received = arg);
        var endpoint = RemoteEndpointKey.Parse("ws://127.0.0.1:9000/cfet2/event/v2");

        InvokePrivate(
            module,
            "RaiseConnectionStatus",
            endpoint,
            ConnectionStatusEvent.Connected,
            "ready",
            0);

        Assert.IsNotNull(received);
        Assert.AreEqual(ConnectionStatusEvent.Source, received.Source);
        Assert.AreEqual(ConnectionStatusEvent.Connected, received.EventType);
        Assert.AreEqual("ws://127.0.0.1:9000", received.Host);
    }

    [TestMethod]
    public void CallbackException_ShouldNotEscapeEventRaising()
    {
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());
        module.RegisterEventCallback(_ => throw new InvalidOperationException("handler failed"));
        var endpoint = RemoteEndpointKey.Parse("ws://127.0.0.1:9000/cfet2/event/v2");

        InvokePrivate(
            module,
            "RaiseConnectionStatus",
            endpoint,
            ConnectionStatusEvent.Connected,
            "ready",
            0);
    }

    [TestMethod]
    public void Dispose_ShouldBeIdempotentAndClearCallback()
    {
        using var module = new WebSocketCommunicationModule(new WebSocketCommunicationOptions());
        var callCount = 0;
        module.RegisterEventCallback(_ => callCount++);
        var endpoint = RemoteEndpointKey.Parse("ws://127.0.0.1:9000/cfet2/event/v2");

        module.Dispose();
        module.Dispose();
        InvokePrivate(
            module,
            "RaiseConnectionStatus",
            endpoint,
            ConnectionStatusEvent.Connected,
            "ready",
            0);

        Assert.AreEqual(0, callCount);
    }

    private static void InvokePrivate(object target, string methodName, params object[] args)
    {
        var method = target.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.IsNotNull(method, $"Missing private method {methodName}.");
        method.Invoke(target, args);
    }

    private static void KillHubIfAlive()
    {
        try
        {
            Hub.KillMaster();
        }
        catch
        {
        }
    }
}
