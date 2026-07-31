using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Attributes;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics
{
    public sealed class WebSocketCMThing : Thing
    {
        private readonly WebSocketCommunicationModule module;

        public WebSocketCMThing(WebSocketCommunicationModule module)
        {
            this.module = module ?? throw new ArgumentNullException(nameof(module));
        }

        [Cfet2Status("status")]
        public WebSocketModuleStatus GetStatus()
        {
            return module.GetStatus();
        }

        [Cfet2Status("printStatus")]
        public string PrintStatus()
        {
            return module.PrintStatus();
        }

        [Cfet2Status("serverSessions")]
        public IReadOnlyList<WebSocketServerSessionStatus> GetServerSessions()
        {
            return module.GetServerSessions();
        }

        [Cfet2Status("printServerSessions")]
        public string PrintServerSessions()
        {
            return module.PrintServerSessions();
        }

        [Cfet2Status("remoteClients")]
        public IReadOnlyList<WebSocketRemoteClientStatus> GetRemoteClients()
        {
            return module.GetRemoteClients();
        }

        [Cfet2Status("printRemoteClients")]
        public string PrintRemoteClients()
        {
            return module.PrintRemoteClients();
        }

        [Cfet2Status("remoteClient")]
        public WebSocketRemoteClientStatus? GetRemoteClient(string hostname)
        {
            return module.GetRemoteClient(hostname);
        }

        [Cfet2Status("printRemoteClient")]
        public string PrintRemoteClient(string hostname)
        {
            return module.PrintRemoteClient(hostname);
        }
    }
}
