# WebSocketCM 实现说明与测试边界 V1.0

本文档定义新的 `WebSocketCommunicationModule`，下文简称 `WebSocketCM`，在当前 CFET2 Core 事件通信模型中的实现要求、协议版本、连接池、订阅生命周期、断线重连、心跳、鉴权、与旧 WebSocket 事件模块的隔离策略，以及必须覆盖的测试边界。

本文档为 V1.0 设计规格。V1.0 的目标是先实现可靠的远程事件订阅、取消订阅、事件接收、连接状态通知和新旧协议隔离；不承诺断线期间事件可靠补偿。

## 1. 背景和目标

旧 `websocketeventthing` 是 `Thing + IRemoteEventHub` 形态，客户端侧以 `Token` 为粒度创建 WebSocket 连接，收到远程事件后直接调用订阅时传入的业务 handler。

当前 CFET2 Core 已经切换到 `IEventCommunicationModule` 模型：

- `Hub.StartCommunication()` 会对每个事件通信模块调用 `RegisterEventCallback(myMaster.MyEventHub.ReceiveRemoteEvent)`。
- `EventHub.Subscribe(...)` 对远程订阅只把 `Token` 和 `EventFilter` 交给事件通信模块，不再把业务 handler 传入通信模块。
- 远程事件必须先回流到 `EventHub.ReceiveRemoteEvent(...)`，再由 `EventHub` 统一匹配 remote subscriber 并调用业务 handler。
- 连接断开、重连、失败必须通过连接状态事件通知业务层，通信模块不得直接调用业务 handler。

因此新的 `WebSocketCM` 不是旧模块的兼容实现，而是新的事件通信模块。旧服务端和旧客户端默认视为不兼容，必须通过版本握手明确失败。

## 2. Core 接口契约

`WebSocketCM` 必须继承 `Jtext103.CFET2.Core.Communication.IEventCommunicationModule`，并实现：

```csharp
public override string[] ProtocolNames { get; }
public override void Start();
public override void Dispose();
public override string GetFullHost(string protocol, string hostname, int port);
public override void Subscribe(Token token, EventFilter filter);
public override void UnSubScribe(Token token);
public override void RegisterEventCallback(Action<EventArg> handler);
```

实现要求：

- `ProtocolNames` 必须包含 `"ws"` 和 `"http"`。
- `"http"` 用于支持 CFET 既有 URI 写法：用户可以用 `http://host:port/resource/path` 订阅事件，模块内部必须转换为 `ws://host:{port + 1}/cfet2/event/v2`。这里的 `"http"` 不代表使用 HTTP 协议传输事件，真实事件传输仍然使用 WebSocket。
- `RegisterEventCallback` 只保存 Core 注入的回调。收到远程事件、连接状态事件时只能调用该回调。
- `Subscribe` 负责远程订阅、连接池映射和 token 状态维护。
- `UnSubScribe` 负责取消远程订阅、本地资源清理和连接释放策略。
- `Dispose` 必须取消全部订阅、释放全部连接、停止接收循环、重连任务、心跳任务和 idle timer。当前连接释放主要使用 `Dispose()`；连接失败或超时时可能使用 `Abort()`，不保证主动发送 WebSocket Close 1000。

## 3. 协议版本和新旧隔离

新的 `WebSocketCM` 必须和旧 `websocketeventthing` 明确隔离。V1.0 使用三层校验。

第一层：独立 endpoint。

```text
ws://host:port/cfet2/event/v2
```

第二层：WebSocket subprotocol。

```text
cfet2-event.v2
```

第三层：应用层 Hello 握手。

客户端连接成功后第一包必须发送 `hello`：

```json
{
  "messageType": "hello",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "clientId": "optional-client-id",
  "authScheme": null,
  "authToken": null
}
```

服务端必须回复 `helloAck`：

```json
{
  "messageType": "helloAck",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "accepted": true,
  "reason": null,
  "heartbeatIntervalMilliseconds": 30000,
  "heartbeatTimeoutMilliseconds": 90000
}
```

`accepted` 表示服务端是否接受本次握手。`accepted = false` 时，`reason` 应返回拒绝原因；客户端将连接标记为 `Failed`，不得发送真实 Subscribe。`accepted = true` 时，`reason` 通常为空。

当前实现已经支持 `heartbeatIntervalMilliseconds` 和可选的 `heartbeatTimeoutMilliseconds`。两项协商均为服务端约束下发：客户端分别使用本地配置与服务端返回值中的较短正值；服务端不采用客户端计算出的有效值，仍使用本地 `HeartbeatInterval` 和 `ServerIdleTimeout`。因此 timeout 协商不表示客户端和服务端最终使用同一个 timeout。

服务端未返回 `heartbeatTimeoutMilliseconds`，或返回值小于等于 0 时，客户端继续使用本地 `HeartbeatTimeout`；客户端本地 timeout 小于等于 0、服务端返回正值时，客户端使用服务端值；双方均未提供正值时，客户端 timeout 检测禁用。interval 使用相同的正值选择规则。

客户端本地 interval 和 timeout 同时为正数时，连接前必须满足 `HeartbeatTimeout >= HeartbeatInterval × 3`，否则标记为配置失败。握手完成后，客户端最终协商出的 interval 和 timeout 同时为正数时，也必须满足三倍约束，否则将远端握手标记为协议失败。服务端启动时要求本地 `ServerIdleTimeout >= HeartbeatInterval × 3`。任一参与校验的 interval 或 timeout 小于等于 0 时，不执行三倍约束校验。

若 endpoint 不存在、subprotocol 不匹配、无 `helloAck`、协议名不匹配、版本不兼容或返回旧格式消息，连接必须进入 `Failed`，真实 Subscribe 不得发送，并通过 `ConnectionStatusEvent.Failed` 通知业务层。

错误信息必须包含远端地址和原因，例如：

```text
WebSocketCM version check failed for ws://127.0.0.1:8081/cfet2/event/v2:
expected cfet2.websocket.event v2, but remote did not complete handshake.
```

## 4. Host 解析和端口映射

`GetFullHost(protocol, hostname, port)` 用于 `EventFilter` 从远程 URI 解析出 `filter.Host`。

推荐规则：

- `ws://host:port/resource/path` 解析为：
  - Core 使用的 `filter.Host = ws://host:port`
  - WebSocket 连接 URI 为 `ws://host:port/cfet2/event/v2`
  - `filter.SourcesAndTypes[0].Source = /resource/path`
- `http://host:port/resource/path` 是 CFET 支持的标准写法，必须映射为：
  - Core 使用的 `filter.Host = ws://host:{port + 1}`
  - WebSocket 连接 URI 为 `ws://host:{port + 1}/cfet2/event/v2`
  - `filter.SourcesAndTypes[0].Source = /resource/path`

CFET 节点占用两个相邻端口：`http` 端口用于资源访问，`ws` 端口用于事件通信，事件端口固定为资源端口 + 1。`WebSocketCM` 必须在 `GetFullHost("http", host, port)` 中执行该转换。

当前实现区分 Core Host 标识与 WebSocket 连接 URI：

```text
Core Host 标识：scheme://host:port
WebSocket 连接 URI：scheme://host:port/cfet2/event/v2
```

`filter.Host`、`token.Host` 和回流的 `EventArg.Host` 使用 Core Host 标识。`RemoteEndpointKey` 作为连接池 key 保存 scheme、host、port 和固定 endpoint path；`RemoteEndpointKey.ToUri()` 用于生成实际连接 URI。

建议建模：

```csharp
public readonly record struct RemoteEndpointKey(
    string Scheme,
    string Host,
    int Port,
    string Path);
```

客户端 `RemoteEndpointKey.Parse(...)` 只接受 `ws` 和 `wss`。其他 scheme 会在创建连接前抛出 `FormatException`，不会进入后台重连。对业务事件回流到 `EventHub` 时，`EventArg.Host` 应与 remote subscriber 的 Core Host 标识保持一致。

## 5. 鉴权扩展点

当前服务端只支持匿名连接，尚未执行鉴权逻辑。鉴权是未来扩展能力，不作为当前 V1.0 完成标准。

当前已预留：

- 客户端可在 HTTP Upgrade 阶段发送 `Authorization` header。
- Hello 消息可携带扁平字段 `authScheme` 和 `authToken`。
- 客户端启用鉴权但未配置 scheme 时，连接进入配置失败。
- `IWebSocketAuthenticator` 和 `AllowAnonymousWebSocketAuthenticator` 作为未来服务端鉴权扩展点。

示例：

```json
{
  "messageType": "hello",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "authScheme": "Bearer",
  "authToken": "opaque-token"
}
```

未来实现服务端鉴权时，鉴权失败不得创建任何本地 EventHub 订阅，鉴权信息也不得写入普通日志。

## 6. 消息协议

V1.0 不复用旧的裸 `EventRequest` 顶层消息。所有消息必须有 envelope：

```json
{
  "messageType": "subscribe",
  "protocol": "cfet2.websocket.event",
  "version": 2
}
```

缺少 `messageType/protocol/version` 的消息一律拒绝。

### 6.1 Subscribe

```json
{
  "messageType": "subscribe",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "tokenId": "bf5e6d89-7d31-4db5-9185-1df3baf8e17a",
  "filter": {
    "host": "ws://127.0.0.1:8081",
    "sourcesAndTypes": [
      {
        "source": "/EventTest/idtest/2",
        "eventType": "Changed"
      }
    ],
    "performanceLevel": 0
  }
}
```

当前 `SubscribeMessage` 尚未实现 `cursor` 字段，也不实现断线补偿。`cursor` 仅作为未来可靠补偿方向保留在设计中。

服务端收到后：

- 校验 `protocol/version/messageType/tokenId/filter`。
- 将远程 filter 转成本地 filter，服务端本地订阅时必须清空 `filter.Host`。
- 通过服务端 `Hub` 建立本地订阅，不直接持有或直接操作 `EventHub`。
- 服务端优先尝试 `Hub.SubscribeResource(source, pushToRemote, eventType)`：
  - source 能解析为本地 Status/Config 资源时，保存 `ResourceSubscription`。
  - 资源带 `[Cfet2Event]` 时，不启动 Core 内部观测，只等待 Thing 主动发布。
  - 资源未带 `[Cfet2Event]` 时，Core 启动内部观测，发现变化后发布 `changed`。
  - source 不存在时，回退为普通事件订阅 `Hub.Subscribe(localFilter, pushToRemote)`。
  - source 存在但不是可订阅资源，或未标记资源订阅非 `changed` 事件时，拒绝订阅，不回退普通事件。
- 按 `sessionId + tokenId` 保存远端订阅关系。
- 聚合项保存实际本地订阅对象；释放时统一 `Dispose()`，该对象可能是 `ResourceSubscription`，也可能是普通 `Token`。
- 返回 `subscribeAck`。

### 6.2 SubscribeAck

```json
{
  "messageType": "subscribeAck",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "tokenId": "bf5e6d89-7d31-4db5-9185-1df3baf8e17a",
  "accepted": true
}
```

失败示例：

```json
{
  "messageType": "subscribeAck",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "tokenId": "bf5e6d89-7d31-4db5-9185-1df3baf8e17a",
  "accepted": false,
  "reason": "invalid filter"
}
```

客户端等待 SubscribeAck 的时限由 `SubscribeAckTimeout` 控制，默认 10 秒；超时后使用同一 MasterToken 重发 Subscribe，最多按 `MaxSubscribeRetryCount` 重试，默认 3 次。重试耗尽后中止当前连接，并进入连接失败与重连处理。远端明确返回 `accepted = false` 时，才将该订阅状态设置为 `Failed` 并记录 `LastError`。

### 6.3 Unsubscribe

```json
{
  "messageType": "unsubscribe",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "tokenId": "bf5e6d89-7d31-4db5-9185-1df3baf8e17a"
}
```

**说明**：V1.0 协议中 Unsubscribe 无远端 Ack。客户端优先清理本地状态，并在发送失败时按 `MaxUnsubscribeRetryCount` 重试，默认最多重试 3 次，间隔由 `UnsubscribeRetryInterval` 控制，默认 100ms。重试耗尽后不恢复本地订阅；如果连接仍有有效订阅，则通过连接重建恢复远端一致性。服务端收到后只取消该 `tokenId` 对应的本地订阅，不得关闭整个连接，除非连接已经没有任何订阅且服务端策略要求关闭空连接。

### 6.4 RemoteEvent

V1.0 明确采用“服务端推送业务事件，客户端 EventHub 二次分发”的模型。服务端不要求维护事件到客户端业务 handler 的一一对应关系，客户端也不得绕过 EventHub 直接按 token 调业务 handler。

事件消息允许携带单个 `tokenId` 作为订阅标识优化信息，但客户端不得依赖它完成业务分发。最终分发必须由 `EventHub.ReceiveRemoteEvent(...)` 后的 remote subscriber 匹配完成。

**注意**：JSON 中的 `tokenId` 字段是**可选的（Optional）**。如果服务端为了极简没有下发，客户端的反序列化器不得抛出异常。V1.0 不再定义 `tokenIds` 数组；如果旧端仍发送该未知字段，客户端应忽略它并继续按 `EventArg` 回流 EventHub。

非去重推送示例：

```json
{
  "messageType": "event",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "tokenId": "bf5e6d89-7d31-4db5-9185-1df3baf8e17a",
  "eventArg": {
    "source": "/EventTest/idtest/2",
    "eventType": "Changed",
    "host": "ws://127.0.0.1:8081",
    "sample": {}
  }
}
```

无 token 广播式推送示例：

```json
{
  "messageType": "event",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "eventArg": {
    "source": "/EventTest/idtest/2",
    "eventType": "Changed",
    "host": "ws://127.0.0.1:8081",
    "sample": {}
  }
}
```

客户端收到后：

- 反序列化为 `EventArg`。
- 将 `EventArg.Host` 设置为当前连接对应的 remote host，确保与订阅 filter 的 Host 一致。
- 如果 `Sample` 不为空，设置 `Sample.IsRemote = true`。
- 只调用 `RegisterEventCallback` 注册的回调。
- 不直接调用业务 handler。

### 6.5 Ping 和 Pong

V1.0 必须实现应用层心跳，防止中间代理、NAT 或防火墙造成半开连接。

```json
{
  "messageType": "ping",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "timestampUtc": "2026-05-26T02:00:00Z"
}
```

```json
{
  "messageType": "pong",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "timestampUtc": "2026-05-26T02:00:00Z"
}
```

心跳要求：

- 客户端主动发送 Ping，服务端收到后回复 Pong；当前服务端不主动发送 Ping。
- 服务端通过 `HelloAck.HeartbeatIntervalMilliseconds` 返回本地 `HeartbeatInterval`，客户端使用本地配置与服务端返回值中的较短正值。
- 服务端通过可选的 `HelloAck.HeartbeatTimeoutMilliseconds` 返回本地 `ServerIdleTimeout`，客户端使用本地 `HeartbeatTimeout` 与服务端返回值中的较短正值。
- 服务端返回字段缺失或值小于等于 0 时，客户端使用对应本地配置；客户端本地值小于等于 0、服务端返回正值时，客户端使用服务端值；双方均无正值时，对应客户端功能禁用。
- interval 和 timeout 的默认值分别为 30s 和 90s；默认 `ServerIdleTimeout` 为 120s。
- 服务端始终使用自身的 `HeartbeatInterval` 和 `ServerIdleTimeout`，不会使用客户端最终计算出的有效值。
- interval 和 timeout 同时为正数时，timeout 必须至少为 interval 的 3 倍。客户端连接前校验本地配置，握手后校验最终协商结果；服务端启动时校验本地配置。非法本地配置产生配置失败，非法握手结果产生协议失败。
- 任意业务消息、ack、error、pong 都可刷新 `lastReceivedAt`。
- **客户端侧**：如果超过协商后的有效 timeout 未收到任何消息，主动关闭连接并进入 `Reconnecting`。主动关闭前应产生 `Disconnected` 或 `Reconnecting` 状态事件。
- **服务端侧**：接收等待超过本地 `ServerIdleTimeout` 时，主动剔除僵尸 session 并清理关联订阅。

### 6.6 Error

```json
{
  "messageType": "error",
  "protocol": "cfet2.websocket.event",
  "version": 2,
  "code": "version_mismatch",
  "reason": "expected v2"
}
```

错误消息不替代连接状态事件。凡是会影响连接、订阅或重连状态的错误，都应同时转换为 `ConnectionStatusEvent.Failed` 或合适状态事件。

## 7. 连接池设计

`WebSocketCM` 当前使用每 endpoint 一个 `RemoteEndpointClient` 的连接池，并通过客户端订阅聚合器将本地订阅映射为远端 MasterToken：

```csharp
private readonly ConcurrentDictionary<RemoteEndpointKey, RemoteEndpointClient> endpointClients;
private readonly ClientSubscriptionAggregator subscriptionAggregator;
```

主要对象职责：

- `ClientSubscriptionAggregator`：维护本地 token、聚合组和 MasterToken 映射。
- `RemoteEndpointClient`：单 endpoint 门面，持有订阅存储、连接状态机、动作队列和 idle release timer。
- `RemoteEndpointConnection`：负责 WebSocket 连接、握手、发送、接收、心跳和重连。
- `RemoteEndpointSubscriptionStore`：维护该 endpoint 下 MasterToken 对应的远端订阅状态。

客户端聚合与连接池规则：

- 多个订阅指向同一 `RemoteEndpointKey` 时，只能创建一个 WebSocket。
- 多个远程端点必须创建不同连接。
- 相同 `endpoint + source + eventType` 的多个本地 token 聚合为一个远端订阅，并使用独立的 `MasterTokenId`。
- 多规则 `EventFilter` 拆分为多个远端单规则订阅。
- 取消最后一个本地 token 时，才发送对应 MasterToken 的 Unsubscribe。
- `PerformanceLevel` 当前不参与客户端聚合键，远端 filter 固定降级为 `PerformanceLevel = 0`。
- Subscribe 和 UnSubScribe 必须线程安全。
- 同一连接上的发送必须串行化，避免并发写 WebSocket。
- 所有网络 I/O 必须有超时。
- 同一 endpoint 上的 Subscribe 和 Unsubscribe 通过无界顺序动作队列串行处理。

## 8. 订阅生命周期

### 8.1 Subscribe 流程

1. 校验 `token`、`filter`、`filter.Host` 和有效的 `filter.SourcesAndTypes`。
2. 从 `filter.Host` 解析 `RemoteEndpointKey`；只接受 `ws` 和 `wss`。
3. `ClientSubscriptionAggregator` 将本地 token 加入对应聚合组，并为新聚合组生成 MasterToken。
4. 多规则 filter 拆分为多个单规则远端 Subscribe 动作。
5. 从 `endpointClients` 获取或创建对应的 `RemoteEndpointClient`，将动作投递到该 endpoint 的顺序动作队列。
6. MasterToken 对应的远端订阅先记录为 `Pending`。
7. 如果连接处于 `Connecting` 或 `Reconnecting`，订阅保持 `Pending`，连接成功后统一 flush。
8. 收到 `subscribeAck.accepted = true` 后将远端订阅状态标为 `Active`。
9. 如果 Subscribe 被远端拒绝，保留远端订阅条目，将状态设置为 `Failed`，并记录 `LastError`；不自动删除本地聚合绑定。
10. 如果 SubscribeAck 超时，使用同一 MasterToken 重发；重试耗尽后中止当前连接并进入重连处理。

离线期间新增订阅的行为必须明确：V1.0 采用 `Pending + 重连后 flush` 策略。

当前订阅状态：

```csharp
public enum SubscriptionState
{
    Pending,
    Active,
    Cancelling,
    Failed
}
```

### 8.2 UnSubScribe 流程

1. 根据本地 `token.Id` 找到 `LocalSubscriptionBinding`。
2. 如果不存在，直接返回，保证幂等。
3. 从对应聚合组移除本地 token。
4. 如果聚合组仍有其他本地 token，不发送远端 Unsubscribe。
5. 如果本地 token 是聚合组最后一个成员，删除聚合组，并向对应 endpoint 动作队列投递 MasterToken 的 Unsubscribe。
6. 本地远端订阅状态先移除，再尽力发送 Unsubscribe。
7. Unsubscribe 发送失败、超时或抛异常，不得恢复已清理的本地状态。
8. Unsubscribe 发送失败时按配置重试；重试耗尽后中止当前连接，并在仍有有效订阅时进入重连处理。
9. 如果连接上还有其他订阅，不得因正常 Unsubscribe 成功而关闭连接。
10. 如果连接订阅数为 0，执行释放策略。

**本地清理优先，网络通知尽力而为。** 这是避免死锁和阻塞业务线程的强约束。

### 8.3 释放策略

支持两种策略，默认建议延迟释放：

- 立即释放：最后一个订阅取消后马上关闭 WebSocket。
- 延迟释放：最后一个订阅取消后启动 idle timer，例如 5 到 30 秒。如果期间有新订阅加入，取消释放。

延迟释放时，idle timer 到期后必须再次确认 endpoint client 仍为空闲，且聚合器中不存在该 endpoint 的本地订阅，才能从 `endpointClients` 移除并释放连接。

当前连接释放主要使用 `Dispose()`；连接失败或超时时可能使用 `Abort()`。当前未主动发送 WebSocket Close 1000，也不保证等待 Close 握手完成。

## 9. 断线、重连和 QoS 策略

当前连接状态：

```csharp
public enum RemoteConnectionState
{
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Failed,
    Disposed
}
```

断线处理：

- 非主动关闭导致 `ReceiveLoop` 结束时，连接进入 `Disconnected`。
- 如果该连接仍有订阅，进入 `Reconnecting` 并启动重连。
- 如果订阅数为 0，停止重连，并按 idle release 策略从连接池移除。
- 重连成功后必须重新 Hello 握手。
- 握手成功后，对该连接下所有仍存在的 `Pending/Active` 订阅重新发送 Subscribe。
- 如果重订阅过程中再次断线，状态机必须回到 `Disconnected/Reconnecting`。

当前退避策略：

- `InitialDelay` 默认 1 秒。
- `FastRetryCount` 默认 5，前 5 次使用相同的 `InitialDelay`。
- 后续按 `BackoffMultiplier` 指数退避，默认倍率为 2。
- `MaxDelay` 默认 30 秒。
- 当前没有随机抖动。
- 只要仍存在有效订阅且未发生永久错误，就持续重试。

重订阅风暴控制：

- V1.0 只定义单条 Subscribe 消息，重连后必须逐条发送 Subscribe。
- 每个 Subscribe 发送之间必须间隔一段时间，默认建议 20ms 到 100ms，具体值应可配置。
- 间隔等待必须可取消；如果期间 token 已取消订阅，不得再发送该 token 的 Subscribe。
- 所有发送都必须有超时，失败后进入重连或失败处理。

V1.0 QoS 声明：

- V1.0 采用 at-most-once 事件投递。
- 断线期间服务端产生的事件可能丢失。
- V1.0 不实现事件缓存、cursor 补偿、ack 重放。
- 当前消息 DTO 不包含 `cursor` 字段；可靠补偿游标仅作为后续版本方向。

当前运行边界与运维约定：

### 9.1 慢客户端和服务端 fan-out 过载

- 服务端通过 `MaxFanOutConcurrency` 限制单次 fan-out 的发送并发度，并通过 `MaxConcurrentFanOutEvents` 限制同时执行的 fan-out 事件数量。
- 当前实现不为每个 session 建立独立的有界发送队列，也不主动识别、隔离或断开慢客户端。
- 慢客户端、网络阻塞或大事件发送可能长时间占用全局 fan-out 执行额度。服务端固定消费者数为 `MaxConcurrentFanOutEvents`，另有 `MaxPendingFanOutEvents` 个等待位置（默认 256，必须大于 0）；等待队列满时丢弃新任务。一个任务表示一个事件向一个聚合订阅组分发。
- 满队列时逐次增加 `FanOutQueueFullDropCount`，`WebSocket fan-out overload` 告警最多每秒一次。等待数降至容量一半或以下输出恢复摘要，短暂恢复不重置告警限频；关闭时补记未报告的丢弃数量。半容量仅是告警恢复阈值，有空位立即接受新任务。
- 当前策略提供有界等待、固定分发并发及丢弃诊断，不保证慢客户端与健康客户端之间的事件隔离，也不保证被丢弃事件的补发或重放。Core 的队列与回调调度保持原行为。
- 运维侧应监控该日志。持续出现时，应排查慢客户端、网络状态、事件大小和发布频率，并根据资源情况调整 fan-out 配置。

队列配置示例（设置在模块启动之前）：

```csharp
var options = new WebSocketCommunicationOptions
{
    MaxConcurrentFanOutEvents = 4, // 固定分发消费者
    MaxPendingFanOutEvents = 256,  // 等待任务容量，不含活动任务
    MaxFanOutConcurrency = 8,      // 每个任务内的接收者发送并发
};
```

`GetStatus()` / `PrintStatus()` 暴露等待数、活动数、容量、并发数、等待峰值、累计满队列丢弃数及关闭弃置数。`FanOutShutdownDiscardCount` 仅统计关闭时仍在等待队列中的任务，不与满队列丢弃混用，也不代表客户端通知数量。消息已成功进入 Core 队列不代表已投递到远端。

排队项捕获入队时接收者，发送前和取得会话发送锁后验证订阅实例；退订、revision 更新和聚合组重建使旧目标失效。固定消费者不保证端到端发送顺序。未启动、启动失败和重复释放均清理消费者；关闭放弃等待项，消费者合计最多等待 5 秒，超时后延迟清理仍在使用的资源。

详见 [WebSocket 有界事件分发队列修改方案](WebSocket有界事件分发队列修改方案.md)。

### 9.2 客户端 endpoint 动作队列

- 每个远程 endpoint 使用一个无界顺序动作队列串行处理 Subscribe 和 Unsubscribe，以保证同一 endpoint 上的动作顺序。
- `ActionQueueWarningThreshold` 是告警阈值，不是队列容量上限；超过阈值时只记录 `WebSocket action queue warning` 错误日志。
- 当前实现不拒绝新动作、不丢弃旧动作，也不自动合并同一 token 的重复动作。
- 如果业务持续产生动作的速度高于处理速度，允许出现动作延迟和内存增长。调用方应避免无意义的高频重复订阅与取消订阅。
- 运维侧应监控动作队列告警。持续出现时，应检查远端连接状态、订阅调用频率和业务侧重复操作。

### 9.3 服务端启动失败恢复

- `Start()` 正常成功后保持幂等；重复调用不会重复启动服务端。
- 如果服务端因端口占用、地址绑定失败或其他启动异常导致 `Start()` 抛出异常，当前实现不保证同一个模块实例可以再次调用 `Start()` 并恢复。
- 启动失败后的处理责任属于宿主：记录异常、释放当前模块实例，并按部署策略终止进程或创建新的模块实例。
- 当前版本不提供模块内部的启动失败重试、状态回滚或自动恢复保证。

状态事件：

- 连接首次成功：`ConnectionStatusEvent.Connected`
- 意外断开：`ConnectionStatusEvent.Disconnected`
- 开始重连或每次重连尝试：`ConnectionStatusEvent.Reconnecting`
- 达到失败条件：`ConnectionStatusEvent.Failed`
- 重连成功：`ConnectionStatusEvent.Connected`

状态事件必须通过 `RegisterEventCallback` 注册的回调进入 `EventHub.ReceiveRemoteEvent(...)`，并使用 `ConnectionStatusEvent.Create(...)` 创建。

## 10. 服务端职责

如果 `WebSocketCM` 同时提供服务端能力，服务端必须实现：

- 监听 `ws://local-host:port/cfet2/event/v2`。
- 当前监听支持 `ws/http`；`wss/https` 为保留能力，尚未实现 TLS 监听。
- 要求或校验 subprotocol `cfet2-event.v2`。
- 完成 Hello 握手后才接受 Subscribe。当前服务端按匿名方式接受连接，不校验客户端携带的鉴权信息。
- 维护每个 WebSocket session 下的多个远程订阅。
- Subscribe 时将远程 filter 转为本地 filter，并调用本地 `MyHub.EventHub.Subscribe(...)`。
- 本地事件触发后发送 `event` envelope 给对应 session。
- 可以按单个 `tokenId` 推送，也可以省略 token 作为广播式事件推送；不再使用 `tokenIds` 批量字段。
- Unsubscribe 只取消指定 tokenId 的本地订阅。
- 同一 session 使用相同 `tokenId` 再次 Subscribe 时，以新订阅替换旧订阅。
- 服务端订阅聚合键包含 `source + eventType + performanceLevel`。
- **僵尸连接剔除**：接收等待超过本地 `ServerIdleTimeout` 时主动终止 session，并清理关联的本地订阅。服务端同时在 helloAck 中将该值作为客户端 timeout 协商约束下发，但不会采用客户端最终计算出的有效 timeout。
- WebSocket session 关闭时，取消该 session 下全部本地订阅。
- 服务端 Dispose 时取消所有 session 下全部本地订阅，并关闭监听。
- 服务端也应实现 ping/pong 或响应客户端 ping。
- 服务端通过 `MaxServerSessions`、`ServerHandshakeTimeout`、`ServerIdleTimeout` 和 `ServerSendTimeout` 限制 session 数量及网络操作耗时。
- 单个 session 发送失败或超时后，终止该 session 并清理其全部订阅。
- fan-out 通过 `MaxFanOutConcurrency` 和 `MaxConcurrentFanOutEvents` 限制并发；等待任务达到 `MaxPendingFanOutEvents` 后丢弃新任务并计数、限频告警。

服务端不得假设一个连接只有一个订阅。

## 11. 序列化要求

默认建议使用 JSON + Newtonsoft.Json，与当前 HTTP 模块和旧 WebSocket 事件代码保持一致。

需要注意：

- `EventArg.Sample` 的反序列化可能涉及 `ISample` 接口，新模块需要明确 sample 的 JSON 表达方式。
- 如果直接序列化 `EventArg`，必须保证反序列化后 `Sample.ObjectVal`、`Sample.IsRemote`、`Source`、`EventType`、`Host` 都可用。
- 当前仅实现 `PerformanceLevel = 0` 的完整 `EventArg` 推送。收到其他值时记录警告并降级为 0。
- `PerformanceLevel` 当前不参与客户端订阅聚合键；轻量 payload 和按性能等级区分客户端聚合属于后续优化。
- **容错与大报文处理**：需要合理配置 ClientWebSocket 和服务端的最大接收缓冲区（如 `ReceiveBufferSize`）。若业务事件包（如包含极大数组或图像数据）超过限制导致底层抛出异常，应记录异常日志并丢弃该消息，切勿让单一的大报文导致整个 `ReceiveLoop` 崩溃退出。

建议第一版先稳定全量 `EventArg` 模式，再增加轻量 payload 模式。

当前实现差距：握手完成后收到非法 JSON、错误协议消息或超大消息时，客户端连接或服务端 session 当前仍可能被关闭。目标行为是记录警告、忽略该条消息并继续处理后续消息；超大分片还需要完整 drain 后再继续接收。相关测试应在实现该容错行为后同步调整。

## 12. 与旧模块的隔离要求

旧 `websocketeventthing` 的不兼容点：

- 旧客户端每个 Token 一个 WebSocket。
- 旧服务端每个 session 只保存一个订阅。
- 旧客户端收到消息后直接调用业务 handler。
- 旧协议没有 version envelope，没有 hello 握手。
- 旧断线重连不通过 `ConnectionStatusEvent` 通知业务层。

新模块必须做到：

- 不连接旧根路径 `/` 作为事件端点。
- 不接受无 envelope 的旧 `EventRequest` 作为 Subscribe。
- 不接受缺少 `protocol/version/messageType` 的消息。
- 版本校验失败时不创建 active subscription。
- 通过 `ConnectionStatusEvent.Failed` 把版本不兼容反馈给业务层。

## 13. 测试边界

测试分为 Core 集成约束、协议握手、鉴权、客户端连接池、订阅生命周期、服务端订阅管理、事件回流、心跳、断线重连、版本隔离和资源释放。

### 13.1 Core 集成测试

必须覆盖：

- `WebSocketCM` 可以注册到 `Hub.TryAddCommunicationModule(IEventCommunicationModule)`。
- `Hub.StartCommunication()` 后 `RegisterEventCallback` 被调用。
- 远程 Subscribe 时，Core 只调用 `WebSocketCM.Subscribe(token, filter)`，业务 handler 不传入模块。
- `token.Dispose()` 会通过 `EventHub.Unsubscribe(...)` 调用 `WebSocketCM.UnSubScribe(token)`。
- `GetFullHost("ws", host, port)` 返回 Core Host 标识 `ws://host:port`。
- `GetFullHost("http", host, port)` 返回 Core Host 标识 `ws://host:{port + 1}`，用于支持 `http://...` 订阅写法。

### 13.2 协议握手测试

必须覆盖：

- 客户端连接后先发送 hello。
- 服务端返回接受的 helloAck 后连接状态进入 `Connected`。
- helloAck 下发服务端 heartbeat interval 和可选 timeout，客户端分别使用本地配置与服务端返回值中的较短正值。
- helloAck 缺少 timeout 或返回非正 timeout 时，客户端回退到本地 timeout。
- 客户端本地 heartbeat 时间组合非法时产生配置失败；最终协商组合非法时产生协议失败。
- 服务端仍使用自身的 `HeartbeatInterval` 和 `ServerIdleTimeout`，不使用客户端最终有效值。
- helloAck 版本不匹配时连接失败。
- 缺少 `protocol/version/messageType` 的消息被拒绝。

### 13.3 鉴权测试

当前边界：

- 无鉴权配置时服务端允许连接。
- 客户端配置的鉴权信息能写入 HTTP Upgrade 的 `Authorization` header。
- 客户端配置的 `authScheme/authToken` 能写入 hello。
- 客户端启用鉴权但未配置 scheme 时产生配置失败。
- 服务端当前不验证鉴权字段，鉴权失败和鉴权失败状态事件不属于当前测试要求。
- 后续实现服务端鉴权扩展时，再补充鉴权成功、拒绝订阅和 `ConnectionStatusEvent.Failed` 测试。

### 13.4 远程 Subscribe 测试

必须覆盖：

- WebSocket 可以完成远程 Subscribe。
- Subscribe 消息包含 `messageType/protocol/version/tokenId/filter`。
- 服务端收到普通事件 Subscribe 后通过 `Hub.Subscribe(...)` 建立本地订阅。
- 服务端收到存在的资源 source 后通过 `Hub.SubscribeResource(...)` 建立 `ResourceSubscription`。
- 服务端收到不存在的资源形态 source 时，不应错误解析为资源，应回退为普通事件订阅。
- 服务端收到存在但不可资源订阅的 source 时，应拒绝订阅，不应回退普通事件。
- `[Cfet2Event]` 资源订阅后不启动内部观测，Thing 主动发布后能收到事件。
- 未标记 `[Cfet2Event]` 的资源订阅后启动内部观测，只改值不主动发布也能收到 `changed`。
- 客户端收到 `subscribeAck.accepted = true` 后订阅状态为 Active。
- Subscribe 被拒绝时保留订阅条目，状态为 Failed，并记录 LastError。
- SubscribeAck 超时后使用同一 MasterToken 重试；重试耗尽时中止连接并进入重连。
- 连接处于 `Connecting/Reconnecting` 时新增 Subscribe 会进入 Pending，并在连接恢复后 flush。

### 13.5 连接池测试

必须覆盖：

- 多个订阅指向同一远程端点时只建立一个 WebSocket 连接。
- 多个远程端点会建立不同连接。
- 同 endpoint 下并发 Subscribe 不会创建重复连接。
- Connecting 阶段大量并发 Subscribe 会挂起并最终发送，不创建多个 socket。
- 同 endpoint 下发送是串行的，不出现 WebSocket 并发写异常。
- 连接池 key 对 host 大小写、尾部斜杠、endpoint path 的处理一致。
- endpoint scheme 只接受 `ws/wss`，其他 scheme 抛出 `FormatException`。
- 相同 `endpoint + source + eventType` 的本地订阅聚合为一个远端 MasterToken。
- 多规则 filter 拆分为多个远端单规则订阅。
- 取消最后一个本地 token 时才发送对应 MasterToken 的 Unsubscribe。
- 不同 `PerformanceLevel` 当前仍聚合到同一客户端远端订阅，并降级为 0。

### 13.6 Unsubscribe 测试

必须覆盖：

- 取消某个订阅不会影响同连接上的其他订阅。
- Unsubscribe 消息只包含目标 tokenId。
- 服务端取消指定 tokenId 的本地 EventHub token。
- 重复 UnSubScribe 同一个 token 不抛异常。
- token 不存在时 UnSubScribe 幂等返回。
- 连接已断开时 UnSubScribe 仍能清理本地状态。
- Unsubscribe 发送超时或抛异常时，本地资源仍被释放。
- Unsubscribe 无 Ack；发送失败时按 `MaxUnsubscribeRetryCount` 和 `UnsubscribeRetryInterval` 重试。
- Unsubscribe 最终失败时，本地订阅保持已清理状态；如果连接仍有有效订阅，连接进入重连以恢复一致性。

### 13.7 最后订阅释放测试

必须覆盖：

- 最后一个订阅取消后，立即释放策略会 Dispose 连接并从连接池移除，不要求发送 Close 1000。
- 延迟释放策略会启动 idle timer。
- idle timer 期间有新订阅加入时取消释放，并复用原连接。
- idle timer 到期后连接关闭并从连接池移除。
- 取消释放竞争测试：timer 即将超时或正在回调时发起新 Subscribe，验证连接不会被错误断开。
- Dispose 时不等待 idle timer，立即关闭所有连接。

### 13.8 事件回流测试

必须覆盖：

- 远程事件到达后只调用 `RegisterEventCallback` 注册的回调。
- 回调指向 `EventHub.ReceiveRemoteEvent(...)` 时，业务 handler 由 EventHub 调用。
- 远程事件的 `Host` 与订阅 filter 的 `Host` 一致时才触发业务 handler。
- 远程事件缺失 Host 时由 WebSocketCM 补齐连接 Host，或被明确丢弃。
- 远程事件 `Sample.IsRemote` 为 true。
- 事件消息带 `tokenId` 或不带 token 时，客户端仍通过 EventHub 二次分发。
- 业务 handler 抛异常不影响后续远程事件处理。

### 13.9 心跳测试

必须覆盖：

- helloAck 下发服务端 heartbeat interval，客户端使用本地值与服务端值中的较短正值。
- helloAck 下发可选 heartbeat timeout，客户端使用本地值与服务端值中的较短正值。
- 服务端 timeout 字段缺失或非正时客户端回退本地值；客户端本地值非正而服务端值有效时使用服务端值。
- 客户端本地配置、客户端最终协商结果和服务端启动配置在 interval、timeout 均为正数时，必须满足 timeout 至少为 interval 的 3 倍。
- 客户端本地配置非法产生配置失败；服务端返回值导致最终组合非法时产生协议失败；服务端本地配置非法时启动失败。
- 每次重连完成 hello/helloAck 后重新计算并校验有效 interval 和 timeout。
- 客户端定期发送 ping。
- 服务端收到 ping 返回 pong。
- 收到业务消息会刷新 `lastReceivedAt`。
- 超过 heartbeat timeout 未收到任何消息时，客户端主动关闭连接并进入 Reconnecting。
- 半开连接不会长期停留在 Connected。
- 服务端会主动断开长期无心跳的客户端 session。

### 13.10 断线和重连测试

必须覆盖：

- WebSocket 意外断开后产生 `Disconnected` 状态事件。
- 仍有订阅时进入 `Reconnecting`。
- 重连成功后产生 `Connected` 状态事件。
- 重连成功后重发所有仍存在的 Subscribe。
- 资源形态 source 的远程订阅在断线重连后仍会重发 Subscribe，订阅数不重复增加，重连后继续收到事件。
- 断线期间服务端尝试发送的事件不做补偿；重连后新事件应正常到达。
- 重连后多个订阅会逐条发送，相邻 Subscribe 之间存在可配置间隔。
- 重连期间新增 token，重连成功后会发送该 token 的 Subscribe。
- 重连期间取消某个 token，重连成功后不会重发该 token。
- 重连期间取消最后一个 token，重连任务停止并释放连接。
- 临时故障超过快速重试阶段后仍按指数退避持续重试，不因达到快速重试次数直接进入 Failed。
- 永久错误时停止重试并产生 `Failed` 状态事件。
- 重连成功但在发完 hello 后、重订阅过程中再次断线，状态机不会卡死。
- 重连退避验证默认参数：初始 1 秒、前 5 次快速重试、倍率 2、最大 30 秒且无随机抖动。
- 重连退避策略应通过可注入 clock/delay provider 测试，避免等待真实时间。

### 13.11 版本隔离测试

必须覆盖：

- 新客户端连接旧根 endpoint `/` 时失败。
- 新客户端连接没有 helloAck 的服务端时失败。
- 新客户端收到错误 version 时失败。
- 新服务端收到旧 `EventRequest` 裸消息时拒绝。
- 版本失败不会创建 active subscription。
- 版本失败会通过 `ConnectionStatusEvent.Failed` 通知业务层。

### 13.12 服务端多订阅测试

如果模块实现服务端，必须覆盖：

- 同一 WebSocket session 上多个 Subscribe 都能建立本地订阅。
- 同一 session 取消一个 tokenId 不影响其他 tokenId。
- 同一 session 关闭时取消该 session 下全部本地订阅。
- 服务端推送事件时只推送给匹配 filter 的 session/token。
- 服务端推送事件时只支持单个 `tokenId`；无 token 时客户端仍由 EventHub 分发。
- 服务端 Dispose 会取消所有本地 EventHub token。
- 同一 session 使用相同 tokenId 重复 Subscribe 时替换旧订阅。
- 服务端聚合键包含 `source + eventType + performanceLevel`。
- 达到 `MaxServerSessions` 时拒绝新 session。
- 握手、空闲接收和发送分别受 `ServerHandshakeTimeout`、`ServerIdleTimeout`、`ServerSendTimeout` 限制。
- session 发送失败或超时后终止并清理该 session 的全部订阅。

### 13.13 QoS 和异常路径测试

必须覆盖：

- 断线期间事件允许丢失，重连后不尝试补偿 V1.0 未实现的历史事件。
- 当前消息 DTO 不包含 `cursor` 字段，也不启用补偿逻辑。
- 重连后重订阅按单条 Subscribe 顺序发送，且相邻发送之间有可配置间隔。
- SendAsync 超时不会导致全局死锁。
- fan-out 等待队列达到 `MaxPendingFanOutEvents` 时丢弃新任务并累计计数，`WebSocket fan-out overload` 告警每秒最多一次；不要求慢客户端与健康客户端隔离。
- 目标行为：接收到超大 JSON 报文时完整 drain 并丢弃该条消息，连接继续工作。当前实现仍可能关闭连接，此测试待实现后调整。
- 目标行为：握手后的非法 JSON 或错误协议消息只记录警告并忽略。当前实现仍可能关闭连接或 session，此测试待实现后调整。
- 服务端返回 error 时客户端订阅状态和连接状态一致。

### 13.14 并发和资源测试

必须覆盖：

- 并发 Subscribe/UnSubScribe 不抛异常。
- Dispose 与 Subscribe 并发时不会残留连接或后台任务。
- Dispose 后 RegisterEventCallback 清空或不再调用。
- Dispose 后收到 WebSocket 消息不会调用回调。
- 大量订阅取消后连接池、订阅字典、定时器、心跳任务不会泄漏。
- endpoint 动作队列超过 `ActionQueueWarningThreshold` 时记录一次告警；不要求限制队列容量或拒绝新动作。
- 服务端正常启动后的重复 `Start()` 保持幂等；不要求启动失败后的同实例自动恢复。

## 14. 第一阶段建议落地顺序

建议按以下顺序实现：

1. 实现 `IEventCommunicationModule` 外壳、`RegisterEventCallback`、`GetFullHost` Core Host 映射。
2. 定义 message envelope、hello/helloAck、ping/pong、subscribe/unsubscribe/event/error DTO。
3. 实现客户端连接池、版本握手和客户端鉴权字段透传；服务端鉴权作为后续扩展。
4. 实现 Subscribe 的 Pending/Active 状态和离线 flush。
5. 实现 UnSubScribe 的本地清理优先策略和发送失败重试。
6. 实现 receive loop，把远程事件送入注册回调；坏消息和超大报文忽略能力作为待补齐项。
7. 实现心跳、半开连接探测及服务端僵尸会话剔除。
8. 实现连接状态事件。
9. 实现断线重连和逐条重订阅，相邻 Subscribe 之间必须有可配置间隔。
10. 实现 idle release 及竞态保护，释放时使用 Dispose/Abort。
11. 如果需要对外提供事件服务端，再实现服务端多订阅模型。
12. 补齐测试边界，尤其是连接池、版本隔离、事件回流、心跳、重连和释放竞态。

## 15. 完成标准

`WebSocketCM` 至少满足以下条件才算 V1.0 可用：

- 远程事件订阅、取消订阅、事件接收完整可用。
- 同一远程 endpoint 多订阅只使用一个 WebSocket。
- 多 endpoint 使用不同 WebSocket。
- 连接建立前有 endpoint、subprotocol、hello 三层版本校验。
- 客户端鉴权字段入口明确；服务端鉴权属于后续扩展。
- 远程事件只经 `RegisterEventCallback` 进入 `EventHub.ReceiveRemoteEvent(...)`。
- 断线、重连、失败都通过 `ConnectionStatusEvent` 通知业务层。
- 客户端通过应用层 ping/pong 和协商后的有效 timeout 发现半开连接；服务端通过本地 `ServerIdleTimeout` 清理僵尸 session。
- Connecting/Reconnecting 期间新增订阅能 Pending 并在恢复后 flush。
- 重连恢复后按单条 Subscribe 顺序重订阅，且相邻订阅发送之间存在可配置间隔。
- SubscribeAck 超时重试和 Unsubscribe 发送失败重试行为明确。
- UnSubScribe 本地清理优先，网络失败不阻塞资源释放。
- 最后一个订阅取消后的释放策略明确且通过竞态测试，使用 Dispose/Abort，不承诺 Close 1000。
- 版本不匹配和旧协议混用时明确失败。
- 服务端 session 数量、握手、空闲接收、发送和 fan-out 均有明确限制。
- 握手后的坏消息和异常大包只影响单条消息、不关闭连接，是当前尚待补齐的容错目标。
- V1.0 明确为 at-most-once 投递，不承诺断线事件补偿。
- `Dispose` 后没有连接、订阅、定时器、心跳任务、重连任务残留。
