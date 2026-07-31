namespace Jtext103.CFET2.WebSocketCommunicationModule.Exceptions
{
    public sealed class TransientConnectionException : WebSocketConnectionException
    {
        public TransientConnectionException(string message)
            : base(message)
        {
        }

        public TransientConnectionException(string message, System.Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
