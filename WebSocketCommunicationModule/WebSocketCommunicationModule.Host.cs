using System;

namespace Jtext103.CFET2.WebSocketCommunicationModule
{
    public partial class WebSocketCommunicationModule
    {
        // no WebSocket endpoint
        public override string GetFullHost(string protocol, string hostname, int port)
        {
            if (string.IsNullOrWhiteSpace(protocol) || string.IsNullOrWhiteSpace(hostname) || port < 0)
            {
                return string.Empty;
            }

            if (string.Equals("http", protocol, StringComparison.OrdinalIgnoreCase))
            {
                return BuildWebSocketHost(hostname, (MyHub?.ActiveEventCommunicationConfiguration ?? standaloneConfiguration).GetWebSocketPort(port));
            }

            if (string.Equals("ws", protocol, StringComparison.OrdinalIgnoreCase))
            {
                return BuildWebSocketHost(hostname, port);
            }

            return string.Empty;
        }

        private static string BuildWebSocketHost(string hostname, int port)
        {
            return new UriBuilder("ws", hostname, port).Uri.ToString().TrimEnd('/');
        }
    }
}
