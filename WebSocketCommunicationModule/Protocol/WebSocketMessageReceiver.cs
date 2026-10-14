using System.Net.WebSockets;
using System.Text;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    internal static class WebSocketMessageReceiver
    {
        // A rejected message must be drained to its boundary before the next message is read.
        // Once rejected, retain no further payload, even if it spans many fragments.
        internal static async Task<string> ReceiveTextAsync(WebSocket socket, int bufferSize,
            int maxMessageBytes, CancellationToken cancellationToken)
        {
            var buffer = new byte[bufferSize];
            using var stream = new MemoryStream();
            string? rejection = null;
            WebSocketReceiveResult result;
            do
            {
                result = await socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                    throw new WebSocketException("WebSocket closed while waiting for a message.");
                if (rejection == null)
                {
                    if (result.MessageType != WebSocketMessageType.Text)
                        rejection = "Only text WebSocket messages are supported.";
                    else if (stream.Length + result.Count > maxMessageBytes)
                        rejection = "WebSocket message exceeds the maximum allowed size.";
                    else
                        stream.Write(buffer, 0, result.Count);
                    if (rejection != null) stream.SetLength(0);
                }
            }
            while (!result.EndOfMessage);

            if (rejection != null) throw new FormatException(rejection);
            return Encoding.UTF8.GetString(stream.ToArray());
        }
    }
}
