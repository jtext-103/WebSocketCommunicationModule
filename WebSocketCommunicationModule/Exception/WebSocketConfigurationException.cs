namespace Jtext103.CFET2.WebSocketCommunicationModule.Exceptions
{
    public sealed class WebSocketConfigurationException : WebSocketConnectionException
    {
        public WebSocketConfigurationException(string message)
            : base(message)
        {
        }

        public WebSocketConfigurationException(string message, System.Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
