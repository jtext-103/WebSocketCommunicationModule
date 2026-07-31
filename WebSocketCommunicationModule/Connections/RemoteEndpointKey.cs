using System;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Connections
{
    public readonly record struct RemoteEndpointKey(
        string Scheme,
        string Host,
        int Port,
        string Path)
    {
        public string NormalizedHost => EventHub.NormalizeHost(ToUri().ToString());

        public Uri ToUri()
        {
            var builder = new UriBuilder(Scheme, Host, Port, Path);
            return builder.Uri;
        }

        public static RemoteEndpointKey Parse(string normalizedHost)
        {
            if (string.IsNullOrWhiteSpace(normalizedHost))
            {
                throw new ArgumentException("Remote endpoint host is required.", nameof(normalizedHost));
            }

            var host = normalizedHost.Trim();
            if (!Uri.TryCreate(host, UriKind.Absolute, out var uri))
            {
                throw new FormatException($"Remote endpoint host is invalid: {normalizedHost}");
            }

            if (!string.Equals(uri.Scheme, "ws", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(uri.Scheme, "wss", StringComparison.OrdinalIgnoreCase))
            {
                throw new FormatException("Remote endpoint scheme must be ws or wss.");
            }

            if (uri.Port < 0)
            {
                throw new FormatException("Remote endpoint host must include a port.");
            }

            var path = uri.AbsolutePath.TrimEnd('/');
            if (!string.IsNullOrEmpty(path) &&
                !string.Equals(path, WebSocketProtocolConstants.EndpointPath, StringComparison.OrdinalIgnoreCase))
            {
                throw new FormatException($"Remote endpoint path must be {WebSocketProtocolConstants.EndpointPath}.");
            }

            return new RemoteEndpointKey(
                uri.Scheme.ToLowerInvariant(),
                uri.Host.ToLowerInvariant(),
                uri.Port,
                WebSocketProtocolConstants.EndpointPath);
        }
    }
}
