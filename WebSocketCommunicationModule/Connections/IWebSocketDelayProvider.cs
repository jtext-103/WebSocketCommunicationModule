namespace Jtext103.CFET2.WebSocketCommunicationModule.Connections
{
    public interface IWebSocketDelayProvider
    {
        Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken);
    }
}
