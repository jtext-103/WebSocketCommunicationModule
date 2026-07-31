namespace Jtext103.CFET2.WebSocketCommunicationModule.Connections
{
    public sealed class ReconnectPolicy
    {
        public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

        public int FastRetryCount { get; set; } = 5;

        public double BackoffMultiplier { get; set; } = 2;

        public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

        public TimeSpan GetDelay(int retryCount)
        {
            if (retryCount <= 0)
            {
                retryCount = 1;
            }

            if (InitialDelay <= TimeSpan.Zero ||
                MaxDelay <= TimeSpan.Zero ||
                BackoffMultiplier <= 0)
            {
                return TimeSpan.Zero;
            }

            var backoffRetryCount = Math.Max(0, retryCount - Math.Max(0, FastRetryCount));
            var delayTicks = InitialDelay.Ticks * Math.Pow(BackoffMultiplier, backoffRetryCount);
            if (double.IsInfinity(delayTicks) || delayTicks >= MaxDelay.Ticks)
            {
                return MaxDelay;
            }

            return TimeSpan.FromTicks(Math.Max(0, (long)delayTicks));
        }
    }
}
