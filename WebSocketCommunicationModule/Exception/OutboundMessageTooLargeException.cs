namespace Jtext103.CFET2.WebSocketCommunicationModule.Exceptions
{
    public sealed class OutboundMessageTooLargeException : Exception
    {
        public OutboundMessageTooLargeException(string message)
            : base(message)
        {
        }
    }
}