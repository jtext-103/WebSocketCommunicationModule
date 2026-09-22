using Jtext103.CFET2.Core.Log;

namespace Jtext103.CFET2.WebSocketCommunicationModule
{
    internal static class WebSocketLog
    {
        private static ICfet2Logger logger = EmptyLogger.Instance;

        internal static ICfet2Logger Logger => Volatile.Read(ref logger);

        internal static void Initialize() => Initialize(Cfet2LogManager.GetLogger("WebSocket"));

        // Also allows isolated tests to install a recording logger without constructing a host.
        internal static void Initialize(ICfet2Logger value) =>
            Interlocked.Exchange(ref logger, value ?? throw new ArgumentNullException(nameof(value)));

        internal sealed class EmptyLogger : ICfet2Logger
        {
            internal static readonly EmptyLogger Instance = new();
            private EmptyLogger() { }
            public void Trace(string message) { }
            public void Debug(string message) { }
            public void Info(string message) { }
            public void Warn(string message) { }
            public void Error(string message) { }
            public void Fatal(string message) { }
        }
    }
}
