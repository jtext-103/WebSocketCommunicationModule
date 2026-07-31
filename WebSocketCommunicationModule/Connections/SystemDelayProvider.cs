namespace Jtext103.CFET2.WebSocketCommunicationModule.Connections
{
    public sealed class SystemDelayProvider : IWebSocketDelayProvider
    {
        public Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
        {
            if (delay <= TimeSpan.Zero)
            {
                return Task.CompletedTask;
            }

            return Task.Delay(delay, cancellationToken);
        }
    }
}
