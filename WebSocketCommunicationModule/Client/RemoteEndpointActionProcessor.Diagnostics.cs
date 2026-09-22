namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed partial class RemoteEndpointActionProcessor
    {
        internal int QueueLength => Math.Max(0, Volatile.Read(ref queuedActionCount));
    }
}
