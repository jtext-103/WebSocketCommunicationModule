using System.Buffers;
using System.Net.WebSockets;
using System.Text;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Protocol
{
    internal static class WebSocketMessageReceiver
    {
        // A rejected message must be drained to its boundary before the next message is read.
        // Once rejected, retain no further payload, even if it spans many fragments.
        internal static Task<string> ReceiveTextAsync(WebSocket socket, int bufferSize,
            int maxMessageBytes, CancellationToken cancellationToken)
        {
            return ReceiveTextAsync(socket, bufferSize, maxMessageBytes, cancellationToken, ArrayPool<byte>.Shared);
        }

        internal static async Task<string> ReceiveTextAsync(WebSocket socket, int bufferSize,
            int maxMessageBytes, CancellationToken cancellationToken, ArrayPool<byte> bufferPool)
        {
            var buffer = bufferPool.Rent(bufferSize);
            MemoryStream? stream = null;
            try
            {
                string? rejection = null;
                WebSocketReceiveResult result;
                do
                {
                    result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer, 0, bufferSize), cancellationToken).ConfigureAwait(false);
                    if (result.MessageType == WebSocketMessageType.Close)
                        throw new WebSocketException("WebSocket closed while waiting for a message.");
                    if (rejection == null)
                    {
                        if (result.MessageType != WebSocketMessageType.Text)
                            rejection = "Only text WebSocket messages are supported.";
                        else if ((stream?.Length ?? 0) + result.Count > maxMessageBytes)
                            rejection = "WebSocket message exceeds the maximum allowed size.";
                        else if (stream == null && result.EndOfMessage)
                            return Encoding.UTF8.GetString(buffer, 0, result.Count);
                        else
                        {
                            stream ??= new MemoryStream();
                            stream.Write(buffer, 0, result.Count);
                        }
                        if (rejection != null) stream?.SetLength(0);
                    }
                }
                while (!result.EndOfMessage);

                if (rejection != null) throw new FormatException(rejection);
                if (!stream!.TryGetBuffer(out var message))
                    throw new InvalidOperationException("Cannot access the received message buffer.");
                return Encoding.UTF8.GetString(message.Array!, message.Offset, message.Count);
            }
            finally
            {
                stream?.Dispose();
                bufferPool.Return(buffer);
            }
        }
    }
}
