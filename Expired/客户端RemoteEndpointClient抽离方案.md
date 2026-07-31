# WSCM 客户端 RemoteEndpointClient 抽离方案

本文档描述如何把当前客户端侧由 `WebSocketCommunicationModule + RemoteConnection` 共同承担的“单远程 endpoint 客户端”职责，抽离为独立的 `RemoteEndpointClient`。

核心目标：

```text
每个远程 Host/Endpoint 对应一个 RemoteEndpointClient
每个 RemoteEndpointClient 内部维护一条 ClientWebSocket
并维护该 endpoint 上所有 master token -> remote filter 订阅状态
```

抽离后，`WebSocketCommunicationModule` 继续负责本地 token 聚合和本地 EventHub 广播；`RemoteEndpointClient` 负责单 endpoint 的连接、协议收发、心跳、重连和远程订阅状态。

## 1. 当前实现分析

当前客户端侧已经具备“每 endpoint 一条 socket”的事实模型：

```text
WebSocketCommunicationModule
  -> connections: ConcurrentDictionary<RemoteEndpointKey, RemoteConnection>

RemoteConnection
  -> ClientWebSocket
  -> Filters: MasterTokenId -> EventFilter
  -> State / ReceiveLoopTask / HeartbeatTask / ReconnectTask
  -> SendLock / ConnectLock / LifetimeCts
```

当前职责分布：

- `WebSocketCommunicationModule.cs`
  - 管理本地 token。
  - 管理 `ClientSubscriptionGroup`。
  - 管理 `localSubscriptionIndex`。
  - 创建或释放 `RemoteConnection`。
  - 把远程订阅写入 `connection.Filters` 和 `subscriptions`。

- `WebSocketCommunicationModule.Client.cs`
  - `EnsureConnectedAsync`。
  - `ConnectAndHandshakeAsync`。
  - `FlushSubscriptionsAsync`。
  - `ReceiveLoopAsync`。
  - `HeartbeatLoopAsync`。
  - `ReconnectLoopAsync`。
  - `SendMessageAsync`。
  - `HandleSubscribeAck` / `HandleRemoteEvent` / `HandleErrorMessage`。

- `RemoteConnection.cs`
  - 作为连接状态数据容器。
  - 持有 socket、任务、锁、状态、订阅 filter。

也就是说，当前“一个远端 client”的概念已经存在，但行为被拆在 `WebSocketCommunicationModule.Client.cs`，状态被放在 `RemoteConnection.cs`，上层聚合又直接操作 `connection.Filters`。

## 2. 抽离目标

抽离后推荐形态：

```text
WebSocketCommunicationModule
  -> endpointClients: ConcurrentDictionary<RemoteEndpointKey, RemoteEndpointClient>
  -> clientSubscriptionGroups
  -> localSubscriptionIndex
  -> Subscribe / UnSubScribe 本地聚合
  -> RemoteEvent -> InvokeEventCallback

RemoteEndpointClient
  -> Endpoint
  -> ClientWebSocket
  -> remoteSubscriptions: MasterTokenId -> RemoteSubscriptionEntry
  -> Connect / Handshake / Flush Subscribe
  -> ReceiveLoop / Heartbeat / Reconnect
  -> Send Unsubscribe
  -> idle release
```

必须保持：

1. 同一个 endpoint 只创建一个 `RemoteEndpointClient`。
2. 一个 `RemoteEndpointClient` 内只维护一条活动 `ClientWebSocket`。
3. 一个 endpoint 上可以有多个 master token 订阅。
4. 本地 token 不进入 `RemoteEndpointClient`。
5. `RemoteEndpointClient` 只认识远程协议层 token，即 `MasterTokenId`。
6. 使用当前 wire protocol：`RemoteEventMessage` 只支持可选 `tokenId`，不再支持 `tokenIds`。
7. 不改变客户端订阅聚合粒度：`endpoint + Source + EventType`。
8. 不改变本地广播方式：远程事件回来后仍由 `WebSocketCommunicationModule` 进入本地 EventHub/回调。
9. `tokenId` 缺失时按广播式远程事件处理；`tokenId` 存在但已不是当前 endpoint client 的有效 master token 时丢弃。
10. 旧端额外发送的 `tokenIds` JSON 字段只作为未知字段忽略，不参与分发和 stale 判断。

## 3. 职责边界

### 3.1 WebSocketCommunicationModule 保留职责

`WebSocketCommunicationModule` 是模块级协调器，保留以下职责：

```text
Start / Dispose
服务端 WebSocketEventServer 生命周期
RegisterEventCallback
Subscribe(Token token, EventFilter filter)
UnSubScribe(Token token)
本地 token -> group/master token 映射
ClientSubscriptionGroup 创建和释放
按 endpoint + Source + EventType 聚合
RemoteEvent 进入本地 callback/EventHub 广播
ConnectionStatusEvent 进入本地 callback/EventHub 广播
```

它不再直接负责：

```text
ClientWebSocket 创建
握手
发送 Subscribe/Unsubscribe
接收循环
心跳循环
重连循环
重订阅 flush
socket 发送锁和连接锁
```

### 3.2 RemoteEndpointClient 新增职责

`RemoteEndpointClient` 是单 endpoint 客户端，负责：

```text
Endpoint
ClientWebSocket 生命周期
单 endpoint 上的 master token 订阅表
连接状态机
Hello/HelloAck 握手
SubscribeMessage / UnsubscribeMessage 发送
SubscribeAckMessage 状态更新
ErrorMessage 状态更新
RemoteEventMessage 上抛
Ping/Pong
Heartbeat timeout
Reconnect
ResetSubscriptionsForReconnect
Idle release
Dispose
```

它不负责：

```text
本地 token
LocalSubscriptionBinding
ClientSubscriptionGroup.LocalTokenIds
endpoint + Source + EventType group 是否复用
本地 EventHub 订阅匹配
```

## 4. 建议新增模型

建议新增目录：

```text
WebSocketCommunicationModule/Client/
```

新增文件：

```text
WebSocketCommunicationModule/Client/RemoteEndpointClient.cs
WebSocketCommunicationModule/Client/RemoteEndpointClientEvents.cs
WebSocketCommunicationModule/Client/RemoteSubscriptionEntry.cs
```

### 4.1 RemoteSubscriptionEntry

当前 `SubscriptionEntry` 可以继续沿用，也可以重命名成更清晰的 `RemoteSubscriptionEntry`。

重命名RemoteSubscriptionEntry

```csharp
public sealed class RemoteSubscriptionEntry
{
    public Guid TokenId { get; init; }
    public RemoteEndpointKey Endpoint { get; init; }
    public EventFilter Filter { get; init; } = new();
    public SubscriptionState State { get; set; }
    public DateTime CreatedAtUtc { get; init; }
    public DateTime? ActivatedAtUtc { get; set; }
    public DateTime? SubscribeSentAtUtc { get; set; }
    public string? LastError { get; set; }
}
```

归属从 `WebSocketCommunicationModule.subscriptions` 迁移到 `RemoteEndpointClient` 内部。

### 4.2 RemoteEndpointClient

建议接口形态：

```csharp
internal sealed class RemoteEndpointClient : IDisposable
{
    public RemoteEndpointKey Endpoint { get; }

    public RemoteConnectionState State { get; }

    public int SubscriptionCount { get; }

    public event Action<RemoteEndpointClient, RemoteEventMessage>? RemoteEventReceived;

    public event Action<RemoteEndpointClient, ConnectionStatusEventArgs>? ConnectionStatusChanged;

    public event Action<RemoteEndpointClient>? Idle;

    public bool IsIdle { get; }

    public RemoteEndpointClient(
        RemoteEndpointKey endpoint,
        WebSocketCommunicationOptions options,
        ICfet2Logger logger);

    public void AddOrUpdateSubscription(Guid masterTokenId, EventFilter remoteFilter);

    public bool RemoveSubscription(Guid masterTokenId);

    public SubscriptionEntry? GetSubscription(Guid masterTokenId);

    public IReadOnlyList<SubscriptionEntry> SnapshotSubscriptions();

    public Task EnsureConnectedAsync();

    public Task SendUnsubscribeAsync(Guid masterTokenId);
}
```

第一版不一定要定义接口，先做内部类即可。

### 4.3 ConnectionStatusEventArgs

可以新增轻量事件参数，避免 `RemoteEndpointClient` 直接依赖 `WebSocketCommunicationModule.RaiseConnectionStatus`。

```csharp
internal sealed class ConnectionStatusEventArgs
{
    public string EventType { get; init; } = "";

    public string Reason { get; init; } = "";

    public int RetryCount { get; init; }
}
```

`WebSocketCommunicationModule` 订阅该事件后调用现有 `RaiseConnectionStatus(...)`。

## 5. 字段迁移

### 5.1 WebSocketCommunicationModule 字段

当前：

```csharp
private readonly ConcurrentDictionary<RemoteEndpointKey, RemoteConnection> connections = new();
private readonly ConcurrentDictionary<Guid, SubscriptionEntry> subscriptions = new();
```

建议改为：

```csharp
private readonly ConcurrentDictionary<RemoteEndpointKey, RemoteEndpointClient> endpointClients = new();
```

`subscriptions` 从模块级字段迁移到 `RemoteEndpointClient` 内部：

```csharp
private readonly object subscriptionLock = new();
private readonly Dictionary<Guid, SubscriptionEntry> subscriptions = new();
```

说明：

- `WebSocketCommunicationModule` 不再拥有所有 endpoint 的远程订阅状态。
- 查询订阅状态时通过 `RemoteEndpointClient.GetSubscription(masterTokenId)`。
- stale remote event 判断也应由 `RemoteEndpointClient` 在收到消息时完成。

### 5.2 RemoteConnection 字段

`RemoteConnection` 当前字段大多会迁移到 `RemoteEndpointClient`：

```text
Endpoint
ClientWebSocket
Filters
State
LifetimeCts
ReceiveLoopTask
ReconnectTask
HeartbeatTask
IdleReleaseTimer
SendLock
ConnectLock
LastReceivedAtUtc
RetryCount
```

迁移完成后有两种选择：

1. 删除 `RemoteConnection`。
2. 保留为 `RemoteEndpointClientState` 或 `RemoteConnectionStateHolder`，只作为私有状态对象。

推荐第一版直接让 `RemoteEndpointClient` 持有这些字段，减少双层间接引用。

## 6. Subscribe 流程调整

当前流程：

```text
Subscribe(token, filter)
  -> endpoint = RemoteEndpointKey.Parse(filter.Host)
  -> groupKeys = endpoint + Source + EventType
  -> GetOrCreateClientSubscriptionGroup
       -> connections.GetOrAdd(endpoint, new RemoteConnection)
       -> connection.Filters[masterTokenId] = remoteFilter
       -> subscriptions[masterTokenId] = Pending
  -> EnsureConnectedAsync(connection)
```

抽离后：

```text
Subscribe(token, filter)
  -> endpoint = RemoteEndpointKey.Parse(filter.Host)
  -> groupKeys = endpoint + Source + EventType
  -> GetOrCreateClientSubscriptionGroup
       -> endpointClients.GetOrAdd(endpoint, CreateRemoteEndpointClient)
       -> client.AddOrUpdateSubscription(masterTokenId, remoteFilter)
  -> client.EnsureConnectedAsync()
```

伪代码：

```csharp
private ClientSubscriptionGroup GetOrCreateClientSubscriptionGroup(
    ClientSubscriptionGroupKey groupKey,
    EventFilter sourceFilter,
    ICollection<RemoteEndpointClient> clientsToEnsure)
{
    if (clientSubscriptionGroups.TryGetValue(groupKey, out var existingGroup))
    {
        return existingGroup;
    }

    var masterTokenId = Guid.NewGuid();
    var remoteFilter = CreateSingleRuleRemoteFilter(sourceFilter, groupKey);
    var group = new ClientSubscriptionGroup
    {
        MasterTokenId = masterTokenId,
        Key = groupKey,
        Endpoint = groupKey.Endpoint,
        RemoteFilter = remoteFilter,
        CreatedAtUtc = DateTime.UtcNow,
    };

    clientSubscriptionGroups[groupKey] = group;

    var client = endpointClients.GetOrAdd(groupKey.Endpoint, CreateRemoteEndpointClient);
    client.AddOrUpdateSubscription(masterTokenId, remoteFilter);
    clientsToEnsure.Add(client);

    return group;
}
```

## 7. UnSubScribe 流程调整

当前流程：

```text
UnSubScribe(token)
  -> 找到 binding
  -> group.LocalTokenIds 移除 local token
  -> group 空：
       clientSubscriptionGroups 移除
       subscriptions 移除
       connection.Filters 移除
       SendUnsubscribeAndScheduleReleaseIfIdleAsync(connection, masterTokenId)
```

抽离后：

```text
UnSubScribe(token)
  -> 找到 binding
  -> group.LocalTokenIds 移除 local token
  -> group 空：
       clientSubscriptionGroups 移除
       endpointClient.RemoveSubscription(masterTokenId)
       锁外 endpointClient.SendUnsubscribeAsync(masterTokenId)
       endpointClient 内部在发送后 ScheduleIdleReleaseIfStillIdle()
```

`WebSocketCommunicationModule` 不再判断 `connection.Filters.IsEmpty`，也不再传递退订瞬间计算出的 `releaseWhenIdle`。
是否 idle 必须由 `RemoteEndpointClient` 在发送 unsubscribe 后、timer 触发时、真正释放前反复检查。

伪代码：

```csharp
if (endpointClients.TryGetValue(group.Endpoint, out var client))
{
    var removed = client.RemoveSubscription(group.MasterTokenId);
    if (removed)
    {
        unsubscribesToSend.Add((client, group.MasterTokenId));
    }
}
```

## 8. RemoteEndpointClient 内部流程

### 8.1 AddOrUpdateSubscription

```csharp
public void AddOrUpdateSubscription(Guid masterTokenId, EventFilter remoteFilter)
{
    lock (subscriptionLock)
    {
        subscriptions[masterTokenId] = new SubscriptionEntry
        {
            TokenId = masterTokenId,
            Endpoint = Endpoint,
            Filter = remoteFilter,
            State = SubscriptionState.Pending,
            CreatedAtUtc = DateTime.UtcNow,
        };
    }

    idleReleaseTimer?.Dispose();
    idleReleaseTimer = null;
}
```

如果后续支持修改同一个 master token 的 filter，需要把旧订阅状态重置为 `Pending`。

### 8.2 RemoveSubscription

```csharp
public bool RemoveSubscription(Guid masterTokenId)
{
    lock (subscriptionLock)
    {
        return subscriptions.Remove(masterTokenId);
    }
}
```

退订只删除内存订阅状态，不返回 `releaseWhenIdle`。
`SendUnsubscribeAsync(...)` 完成后再调用 `ScheduleIdleReleaseIfStillIdle()`，该方法必须重新读取当前订阅表。

```csharp
private void ScheduleIdleReleaseIfStillIdle()
{
    if (!IsIdle)
    {
        return;
    }

    idleReleaseTimer?.Dispose();
    idleReleaseTimer = null;

    if (options.ReleaseConnectionImmediatelyWhenIdle || options.IdleReleaseDelay <= TimeSpan.Zero)
    {
        RaiseIdleIfStillIdle();
        return;
    }

    idleReleaseTimer = new Timer(_ => RaiseIdleIfStillIdle(), null, options.IdleReleaseDelay, Timeout.InfiniteTimeSpan);
}
```

### 8.3 EnsureConnectedAsync

从现有 `WebSocketCommunicationModule.Client.cs` 搬入 `RemoteEndpointClient`：

```text
EnsureConnectedAsync
ConnectAndHandshakeAsync
FlushSubscriptionsAsync
ReceiveLoopAsync
HeartbeatLoopAsync
ReconnectLoopAsync
ResetSubscriptionsForReconnect
SendMessageAsync
ReceiveTextMessageAsync
CreateTimeoutTokenSource
MarkConnectionFailed
MarkSubscriptionsFailed
```

差异点：

- 不再访问 `WebSocketCommunicationModule.subscriptions`，改用自身 `subscriptions`。
- 不再直接调用 `RaiseRemoteEvent`，改为触发 `RemoteEventReceived`。
- 不再直接调用 `RaiseConnectionStatus`，改为触发 `ConnectionStatusChanged`。
- 不再调用模块的 `ObserveBackgroundTask`，可以把该 helper 移入 `RemoteEndpointClient` 或新增公共内部 helper。

### 8.4 HandleRemoteEvent

stale token 判断放到 client 内部更自然。当前协议只定义可选 `tokenId`：

- `tokenId == null`：按广播式远程事件处理，继续回流 EventHub。
- `tokenId != null` 且仍存在于当前 endpoint client 的订阅表：继续回流 EventHub。
- `tokenId != null` 但已被退订或从未存在：丢弃，避免 stale remote event。
- 旧端额外发送的 `tokenIds` JSON 字段由反序列化器忽略，不进入该判断。

```csharp
private void HandleRemoteEvent(RemoteEventMessage message)
{
    if (disposed || message.EventArg == null)
    {
        return;
    }

    if (message.TokenId.HasValue &&
        !ContainsSubscription(message.TokenId.Value))
    {
        return;
    }

    RaiseRemoteEventReceived(message);
}
```

`WebSocketCommunicationModule` 收到事件后：

```csharp
private void OnRemoteEventReceived(RemoteEndpointClient client, RemoteEventMessage message)
{
    if (message.EventArg == null)
    {
        return;
    }

    RaiseRemoteEvent(client.Endpoint, message.EventArg);
}
```

注意：当前 `RaiseRemoteEvent` 参数是 `RemoteConnection connection`，抽离后应改成：

```csharp
private void RaiseRemoteEvent(RemoteEndpointKey endpoint, EventArg eventArg)
```

## 9. 事件回调接线

创建 `RemoteEndpointClient` 时，模块需要接线：

```csharp
private RemoteEndpointClient CreateRemoteEndpointClient(RemoteEndpointKey endpoint)
{
    var client = new RemoteEndpointClient(endpoint, options, logger);

    client.RemoteEventReceived += OnRemoteEventReceived;
    client.ConnectionStatusChanged += OnConnectionStatusChanged;
    client.Idle += OnClientIdle;

    return client;
}
```

### 9.1 RemoteEventReceived

```csharp
private void OnRemoteEventReceived(RemoteEndpointClient client, RemoteEventMessage message)
{
    if (message.EventArg == null)
    {
        return;
    }

    RaiseRemoteEvent(client.Endpoint, message.EventArg);
}
```

### 9.2 ConnectionStatusChanged

```csharp
private void OnConnectionStatusChanged(
    RemoteEndpointClient client,
    ConnectionStatusEventArgs args)
{
    RaiseConnectionStatus(client.Endpoint, args.EventType, args.Reason, args.RetryCount);
}
```

### 9.3 Idle client 移除

当前 `ReleaseConnectionIfIdle` 在模块里从 `connections` 字典移除 connection。

抽离后可以有两种做法：

1. `RemoteEndpointClient` 自己在 idle 时 Dispose，但通知模块从字典移除。
2. `RemoteEndpointClient` 暴露 `IsIdle`，模块继续负责移除。

推荐第一版采用事件通知：

```csharp
public event Action<RemoteEndpointClient>? Idle;
```

模块处理：

```csharp
private void OnClientIdle(RemoteEndpointClient idleClient)
{
    if (endpointClients.TryGetValue(idleClient.Endpoint, out var current) &&
        ReferenceEquals(current, idleClient) &&
        idleClient.IsIdle)
    {
        endpointClients.TryRemove(idleClient.Endpoint, out _);
        idleClient.Dispose();
    }
}
```

不能只按 endpoint 删除，因为旧 client 的 idle timer 可能晚于新订阅创建。
删除前必须确认字典里仍是触发 idle 的同一个实例，并且该实例仍然 idle。

## 10. Dispose 设计

### 10.1 WebSocketCommunicationModule.Dispose

抽离后：

```csharp
foreach (var client in endpointClients.Values)
{
    client.RemoteEventReceived -= OnRemoteEventReceived;
    client.ConnectionStatusChanged -= OnConnectionStatusChanged;
    client.Idle -= OnClientIdle;
    client.Dispose();
}

endpointClients.Clear();
clientSubscriptionGroups.Clear();
localSubscriptionIndex.Clear();
```

模块不需要逐个远程 unsubscribe，因为 socket 即将释放。

### 10.2 RemoteEndpointClient.Dispose

负责：

```text
设置 disposed
取消 LifetimeCts
停止 idle timer
Dispose ClientWebSocket
清空 subscriptions
Dispose SendLock / ConnectLock / LifetimeCts
```

注意：

- Dispose 不应等待 receive loop / heartbeat loop 长时间结束。
- Dispose 不应发送 unsubscribe。
- Dispose 后事件回调不应再触发。

建议所有事件触发统一走内部 helper，避免后台 receive/reconnect/heartbeat loop 在 Dispose 后继续上抛：

```csharp
private void RaiseRemoteEventReceived(RemoteEventMessage message)
{
    if (disposed)
    {
        return;
    }

    RemoteEventReceived?.Invoke(this, message);
}

private void RaiseConnectionStatusChanged(string eventType, string reason, int retryCount)
{
    if (disposed)
    {
        return;
    }

    ConnectionStatusChanged?.Invoke(this, new ConnectionStatusEventArgs
    {
        EventType = eventType,
        Reason = reason,
        RetryCount = retryCount,
    });
}

private void RaiseIdleIfStillIdle()
{
    if (disposed || !IsIdle)
    {
        return;
    }

    Idle?.Invoke(this);
}
```

`WebSocketCommunicationModule.Dispose` 建议显式解绑事件后再 Dispose client：

```csharp
foreach (var client in endpointClients.Values)
{
    client.RemoteEventReceived -= OnRemoteEventReceived;
    client.ConnectionStatusChanged -= OnConnectionStatusChanged;
    client.Idle -= OnClientIdle;
    client.Dispose();
}
```

### 10.3 connection generation

`RemoteEndpointClient` 每次创建新的 `ClientWebSocket` 时建议递增 generation：

```csharp
private long connectionGeneration;
```

receive loop / heartbeat loop 启动时捕获当前 generation：

```csharp
var generation = connectionGeneration;
receiveLoopTask = ObserveBackgroundTask(ReceiveLoopAsync(generation));
heartbeatTask = ObserveBackgroundTask(HeartbeatLoopAsync(generation));
```

旧 loop 只有在 generation 仍匹配时才能修改连接状态、触发 reconnect 或上抛状态事件。
这样可以避免旧 socket 的异常晚到后覆盖新连接状态。

## 11. 并发契约

`RemoteEndpointClient` 迁入 `subscriptions` 后，不能只依赖 `ConcurrentDictionary`。
`SubscriptionEntry.State`、`LastError`、`SubscribeSentAtUtc` 等字段是可变状态，需要明确修改边界。

建议使用：

```csharp
private readonly object subscriptionLock = new();
private readonly Dictionary<Guid, SubscriptionEntry> subscriptions = new();
```

规则：

```text
1. AddOrUpdateSubscription / RemoveSubscription / GetSubscription / SnapshotSubscriptions 在 subscriptionLock 内访问订阅表。
2. HandleSubscribeAck / HandleErrorMessage / ResetSubscriptionsForReconnect 在 subscriptionLock 内修改 entry 状态。
3. IsIdle 在 subscriptionLock 内判断 subscriptions.Count == 0。
4. 锁内只改内存，不 await 网络 IO。
5. FlushSubscriptionsAsync 先在锁内取 Pending snapshot，锁外发送 SubscribeMessage。
6. Subscribe 发送成功或失败后，回到锁内确认 token 仍存在且仍是同一次 pending 发送，再更新 SubscribeSentAtUtc / LastError。
7. SubscribeAck 到达时，如果 token 已退订，直接忽略。
8. ErrorMessage 到达时，如果 token 已退订，直接忽略。
9. Reconnect reset 只重置仍存在且 Active/Pending 的 token。
10. SendUnsubscribeAsync 只发送协议消息；发送完成后调用 ScheduleIdleReleaseIfStillIdle。
```

`WebSocketCommunicationModule` 的 `clientSubscriptionLock` 仍只保护本地聚合索引。
模块锁内可以调用 `client.AddOrUpdateSubscription(...)` 和 `client.RemoveSubscription(...)`，但不能 await `EnsureConnectedAsync` / `SendUnsubscribeAsync`。

## 12. 测试调整

### 12.1 现有测试需要调整的反射字段

当前测试可能反射：

```text
connections
subscriptions
RemoteConnection.Filters
RemoteConnection.State
```

抽离后应调整为：

```text
endpointClients
RemoteEndpointClient.SubscriptionCount
RemoteEndpointClient.State
RemoteEndpointClient.GetSubscription(masterTokenId)
```

建议为测试增加统一 helper，避免每个测试直接反射内部字段：

```csharp
private static IReadOnlyList<object> GetEndpointClients(WebSocketCommunicationModule module)
private static SubscriptionEntry? GetSubscriptionForLocalToken(WebSocketCommunicationModule module, Guid localTokenId)
private static Guid[] GetMasterTokenIds(WebSocketCommunicationModule module, Guid localTokenId)
```

### 12.2 必须保留的行为测试

抽离不应改变以下行为：

1. 同 endpoint 多订阅只建立一条 WebSocket。
2. 不同 endpoint 建立不同 WebSocket。
3. Subscribe 后发送 Hello，再发送 Subscribe。
4. SubscribeAck accepted 后订阅状态变 Active。
5. SubscribeAck rejected 后订阅状态变 Failed。
6. 心跳 Ping/Pong 正常。
7. 心跳超时重连。
8. 服务端关闭后重连。
9. 重连后只按 master token 重订阅。
10. UnSubScribe 非最后一个本地 token 时不发送远程 unsubscribe。
11. UnSubScribe 最后一个本地 token 时发送 master token unsubscribe。
12. unsubscribe 发送后如果仍 idle，则延迟释放 endpoint client。
13. idle timer 触发前重新 Subscribe，不释放 endpoint client。
14. 旧 client 的 idle 事件晚到，不移除同 endpoint 的新 client。
15. RemoteEvent 不带 `tokenId` 时仍进入本地广播。
16. RemoteEvent 带有效 `tokenId` 时仍进入本地广播。
17. stale token RemoteEvent 被丢弃。
18. 旧 JSON 字段 `tokenIds` 被忽略，不参与分发和 stale 判断。

### 12.3 新增 RemoteEndpointClient 单元测试

如果 `RemoteEndpointClient` 是 internal class，可以通过 `InternalsVisibleTo` 或测试项目反射测试。

建议新增：

```text
TestCode/WebSocketRemoteEndpointClientTests.cs
```

覆盖：

1. `AddOrUpdateSubscription` 创建 pending subscription。
2. `RemoveSubscription` 删除订阅，但不负责计算传出式 `releaseWhenIdle`。
3. `EnsureConnectedAsync` 建立连接并 flush pending subscription。
4. `SubscribeAck` 更新对应 master token 状态。
5. `ErrorMessage` 更新对应 master token 状态。
6. `RemoteEventMessage` 触发 `RemoteEventReceived`。
7. stale token remote event 不触发事件。
8. heartbeat timeout 后触发 reconnect。
9. reconnect 后 reset pending 并重发 subscribe。
10. flush subscribe 过程中退订，后续 ack 不应把已删除订阅标成 Active。
11. Dispose 清理 socket 和 subscriptions。
12. Dispose 后 receive/reconnect/heartbeat loop 不再触发事件。

## 13. 分步实施建议

不要一次性大改。推荐按以下顺序：

1. 新增 `RemoteEndpointClient`，先吸收或包装 `RemoteConnection` 字段。
2. 搬迁连接、握手、收发、心跳、重连方法，保持行为不变。
3. 一次性把远程订阅表迁入 `RemoteEndpointClient`，避免模块级 `subscriptions` 和 client 内 `subscriptions` 长期双写。
4. 增加 client 事件：`RemoteEventReceived`、`ConnectionStatusChanged`、`Idle`，事件参数传 client 实例。
5. 把 `RaiseRemoteEvent(RemoteConnection, EventArg)` 改成 `RaiseRemoteEvent(RemoteEndpointKey, EventArg)`。
6. 重做 idle release：client 内部调度，模块按实例校验后从 `endpointClients` 移除。
7. 删除或废弃 `RemoteConnection`。
8. 调整测试反射 helper，并新增并发/idle/Dispose 测试。
9. 跑完整 `dotnet test`。

每一步都应保持测试可运行。尤其第 3 步、第 4 步和第 6 步风险最高，建议小步提交。

## 14. 风险与边界

### 14.1 事件回调循环引用

`WebSocketCommunicationModule` 持有 `RemoteEndpointClient`，client 又通过事件持有模块方法引用。

这在模块生命周期内可接受，但 Dispose 时建议解绑事件或直接 Dispose 后清空 `endpointClients`。如果后续 client 可能脱离模块长期存在，则需要显式解绑：

```csharp
client.RemoteEventReceived -= OnRemoteEventReceived;
client.ConnectionStatusChanged -= OnConnectionStatusChanged;
client.Idle -= OnClientIdle;
```

### 14.2 锁边界

模块级 `clientSubscriptionLock` 仍只保护本地聚合索引。

`RemoteEndpointClient` 内部应使用自己的 `subscriptionLock` 保护 remote subscriptions 和 entry 状态字段。模块锁内可以调用 `client.AddOrUpdateSubscription(...)` 和 `client.RemoveSubscription(...)`，但不能 await 网络 IO。

继续保持：

```text
锁内只改内存
锁外 EnsureConnectedAsync / SendUnsubscribeAsync
```

### 14.3 重连期间退订

`RemoteEndpointClient.ReconnectLoopAsync` 应以自身 `IsIdle` 作为退出条件。退订最后一个 master token 后，重连循环应自然退出。

### 14.4 状态查询

模块不再直接持有 `subscriptions` 后，测试和诊断需要通过 endpoint client 查询 master token 状态。

建议给 `RemoteEndpointClient` 保留 internal 诊断属性：

```csharp
internal IReadOnlyList<SubscriptionEntry> SnapshotSubscriptions()
```

避免测试过度依赖私有字段。

### 14.5 命名

推荐名称：

```text
RemoteEndpointClient
```

原因：

- 强调一个实例对应一个远程 endpoint。
- 避免和系统级 `ClientWebSocket` 混淆。
- 避免泛泛的 `WebSocketClient`，因为模块同时有服务端和客户端职责。

## 15. 结论

抽离 `RemoteEndpointClient` 是值得做的。

当前代码已经是“每 endpoint 一个连接”的模型，但状态和行为分散在 `WebSocketCommunicationModule.Client.cs` 与 `RemoteConnection.cs` 中。抽离后可以形成更清晰的三层边界：

```text
WebSocketCommunicationModule
  本地 token 语义、本地订阅聚合、本地广播

RemoteEndpointClient
  单 endpoint socket、协议收发、心跳、重连、远程订阅状态

Protocol / Connections models
  消息 DTO、endpoint key、subscription entry、group key
```

这会降低 `WebSocketCommunicationModule` 的复杂度，也能让连接状态机获得更独立、更直接的测试覆盖。

第一版抽离应保持语义不变，只移动职责和状态，不改变协议、不改变聚合 key、不改变本地广播方式。
