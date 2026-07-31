using System;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Connections
{
    internal static class HeartbeatTiming
    {
        internal const int MinimumTimeoutMultiplier = 3;

        public static TimeSpan ResolvePositiveMinimum(TimeSpan localValue, int? remoteMilliseconds)
        {
            if (!remoteMilliseconds.HasValue || remoteMilliseconds.Value <= 0)
            {
                return localValue;
            }

            var remoteValue = TimeSpan.FromMilliseconds(remoteMilliseconds.Value);
            if (localValue <= TimeSpan.Zero)
            {
                return remoteValue;
            }

            return localValue <= remoteValue
                ? localValue
                : remoteValue;
        }

        public static bool IsValid(TimeSpan interval, TimeSpan timeout)
        {
            if (interval <= TimeSpan.Zero || timeout <= TimeSpan.Zero)
            {
                return true;
            }

            return timeout.Ticks >= interval.Ticks * MinimumTimeoutMultiplier;
        }

        public static string DescribeInvalidCombination(TimeSpan interval, TimeSpan timeout)
        {
            return $"Heartbeat timeout ({timeout.TotalMilliseconds} ms) must be at least " +
                $"{MinimumTimeoutMultiplier} times the heartbeat interval ({interval.TotalMilliseconds} ms).";
        }
    }
}
