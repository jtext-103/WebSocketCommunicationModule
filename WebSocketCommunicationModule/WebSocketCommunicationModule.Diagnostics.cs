using Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics;

namespace Jtext103.CFET2.WebSocketCommunicationModule
{
    public partial class WebSocketCommunicationModule
    {
        public WebSocketModuleStatus GetStatus()
        {
            return new WebSocketModuleStatus();
        }

        public string PrintStatus()
        {
            return string.Empty;
        }

        public IReadOnlyList<WebSocketServerSessionStatus> GetServerSessions()
        {
            return Array.Empty<WebSocketServerSessionStatus>();
        }

        public string PrintServerSessions()
        {
            return string.Empty;
        }

        public IReadOnlyList<WebSocketRemoteClientStatus> GetRemoteClients()
        {
            return Array.Empty<WebSocketRemoteClientStatus>();
        }

        public string PrintRemoteClients()
        {
            return string.Empty;
        }

        public WebSocketRemoteClientStatus? GetRemoteClient(string hostname)
        {
            return null;
        }

        public string PrintRemoteClient(string hostname)
        {
            return string.Empty;
        }
    }
}
