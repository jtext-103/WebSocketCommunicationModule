namespace Jtext103.CFET2.WebSocketCommunicationModule.Exceptions
{
    public abstract class WebSocketConnectionException : System.Exception
    {
        protected WebSocketConnectionException(string message)
            : base(message)
        {
        }

        protected WebSocketConnectionException(string message, System.Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
