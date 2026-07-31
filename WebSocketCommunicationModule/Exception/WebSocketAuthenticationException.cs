namespace Jtext103.CFET2.WebSocketCommunicationModule.Exceptions
{
    public sealed class WebSocketAuthenticationException : WebSocketConnectionException
    {
        public WebSocketAuthenticationException(string message)
            : base(message)
        {
        }

        public WebSocketAuthenticationException(string message, System.Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
