using System;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Options
{
    public sealed class WebSocketCommunicationOptions
    {
        public Uri? ListenUri { get; set; }

        public bool EnableServer { get; set; }

        public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);

        public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(10);

        public TimeSpan ServerSendTimeout { get; set; } = TimeSpan.FromSeconds(10);

        public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);

        public TimeSpan ServerHandshakeTimeout { get; set; } = TimeSpan.FromSeconds(30);

        public TimeSpan ServerIdleTimeout { get; set; } = TimeSpan.FromSeconds(120);

        public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);

        public TimeSpan HeartbeatTimeout { get; set; } = TimeSpan.FromSeconds(90);

        public TimeSpan IdleReleaseDelay { get; set; } = TimeSpan.FromSeconds(10);

        public TimeSpan ResubscribeInterval { get; set; } = TimeSpan.FromMilliseconds(50);

        public TimeSpan SubscribeAckTimeout { get; set; } = TimeSpan.FromSeconds(10);

        public int MaxSubscribeRetryCount { get; set; } = 3;

        public int MaxUnsubscribeRetryCount { get; set; } = 3;

        public TimeSpan UnsubscribeRetryInterval { get; set; } = TimeSpan.FromMilliseconds(100);

        public int ReceiveBufferSize { get; set; } = 64 * 1024;

        public int MaxMessageBytes { get; set; } = 4 * 1024 * 1024;

        public int MaxFanOutConcurrency { get; set; } = Math.Max(1, Environment.ProcessorCount * 2);

        public int MaxConcurrentFanOutEvents { get; set; } = Math.Max(1, Environment.ProcessorCount);

        public int MaxServerSessions { get; set; } = 1024;

        public int ActionQueueWarningThreshold { get; set; } = 1000;

        public bool ReleaseConnectionImmediatelyWhenIdle { get; set; }

        public WebSocketAuthOptions Auth { get; set; } = new();

        public ReconnectPolicy ReconnectPolicy { get; set; } = new();

        public IWebSocketDelayProvider DelayProvider { get; set; } = new SystemDelayProvider();
    }
}
