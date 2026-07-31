namespace Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions
{
    internal readonly record struct ServerSubscribeResult(
    int Revision,
    bool Accepted,
    string? Reason);
}