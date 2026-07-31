namespace Jtext103.CFET2.WebSocketCommunicationModule.Exceptions
{
    public sealed class WebSocketProtocolException : WebSocketConnectionException
    {
        public WebSocketProtocolException(string message)
            : base(message)
        {
        }

        public WebSocketProtocolException(string message, System.Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
