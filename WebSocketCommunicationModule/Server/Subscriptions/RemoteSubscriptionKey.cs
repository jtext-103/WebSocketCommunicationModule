using System;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions
{
    internal readonly record struct RemoteSubscriptionKey(
        Guid SessionId,
        Guid RemoteTokenId);
}
