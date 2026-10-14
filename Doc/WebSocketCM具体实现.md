# WebSocketCM 具体实现

## 当前实现与设计建议的区别（2026-09-21）

本文保留初始设计、建议目录或实施步骤。当前已实现客户端/服务端订阅聚合、Level 0/1 载荷、Revision upsert、ACK 状态处理及断线重订阅；“建议”字段和目录不一定是现有公开 API，验收条目也不代表本次已执行测试。

实际入口为 `WebSocketCommunicationModule`，客户端使用 `Client/RemoteEndpointClient` 等组件，服务端使用 `Server/Subscriptions/WebSocketSubscriptionManager`；模块选项以 `Options/WebSocketCommunicationOptions.cs` 为准。`SupportedPerformanceLevels`、`AllowPerformanceLevelFallback` 是设计建议，不是当前可设置的选项。

模块 `ResubscribeInterval` 默认 50 ms；CFET2APP 使用 `GlobalConfig.WebSocketResubscribeIntervalMs` 覆盖，默认 10 ms、允许 0。诊断及状态的当前语义见 [WebSocketCMThing 文档](WebSocketCMThing设计文档.md)，类型边界和失败 ACK 日志见 [README](../README.md)。

本文档基于 `WebSocketCM 实现说明与测试边界 V1.0`，进一步拆分 `WebSocketCommunicationModule` 的具体实现类、文件结构、核心职责和第一阶段功能落地顺序。本文不替代 V1.0 规格；V1.0 仍是协议和测试边界的最终约束。

## 1. 实现目标

`WebSocketCM` 第一版实现目标是：

1. 作为 `IEventCommunicationModule` 注册到 CFET2 Core。
2. 支持远程事件订阅、取消订阅、事件接收和连接状态事件回流。
3. 同一远程 endpoint 下多个订阅复用一个 WebSocket 连接。
4. 使用 `/cfet2/event/v2`、`cfet2-event.v2`、`hello/helloAck` 三层机制隔离旧 WebSocket 事件模块。
5. 支持 `Pending -> Active` 订阅状态、断线重连后逐条重订阅、心跳和本地清理优先的取消订阅。
6. V1.0 明确采用 at-most-once 投递，不实现断线事件补偿。

## 2. 建议目录结构

建议将当前单文件壳拆分为以下结构：

```text
WebSocketCommunicationModule/
  WebSocketCommunicationModule.cs
  WebSocketCommunicationModule.Client.cs
  WebSocketCommunicationModule.Server.cs
  WebSocketCommunicationModule.Host.cs
  WebSocketCommunicationModule.Events.cs
  Options/
    WebSocketCommunicationOptions.cs
    WebSocketAuthOptions.cs
  Connections/
    RemoteEndpointKey.cs
    RemoteConnection.cs
    RemoteConnectionState.cs
    SubscriptionEntry.cs
    SubscriptionState.cs
    ReconnectPolicy.cs
    IWebSocketDelayProvider.cs
    SystemDelayProvider.cs
  Protocol/
    WebSocketProtocolConstants.cs
    WebSocketMessageEnvelope.cs
    HelloMessage.cs
    HelloAckMessage.cs
    SubscribeMessage.cs
    SubscribeAckMessage.cs
    UnsubscribeMessage.cs
    RemoteEventMessage.cs
    HeartbeatMessage.cs
    ErrorMessage.cs
    WebSocketMessageSerializer.cs
  Server/
    WebSocketEventServer.cs
    WebSocketSession.cs
    ServerSubscriptionEntry.cs
  Auth/
    IWebSocketAuthenticator.cs
    AllowAnonymousWebSocketAuthenticator.cs
```

第一阶段可以只创建真正需要的文件；目录边界建议先定下来，避免后续客户端、服务端、协议 DTO 和状态机混在一个类里。

## 3. 外层模块类

### 3.1 `WebSocketCommunicationModule`

职责：

1. 实现 `IEventCommunicationModule` 契约。
2. 保存 Core 注入的 `Action<EventArg>` 回调。
3. 持有连接池、订阅表、配置和可选服务端实例。
4. 负责 `Start()`、`Dispose()` 的生命周期编排。

核心字段建议：

```csharp
private readonly WebSocketCommunicationOptions options;
private readonly ConcurrentDictionary<RemoteEndpointKey, RemoteConnection> connections = new();
private readonly ConcurrentDictionary<Guid, SubscriptionEntry> subscriptions = new();
private readonly object callbackLock = new();
private Action<EventArg>? eventCallback;
private WebSocketEventServer? server;
private volatile bool disposed;
```

接口行为：

1. `ProtocolNames` 默认返回 `["ws", "http"]`。`"http"` 用于支持 CFET 既有 URI 写法，模块内部必须转换为 WebSocket 事件端点；它不表示使用 HTTP 协议传输事件。
2. `Start()` 幂等。若配置启用服务端，则启动 `WebSocketEventServer`。
3. `RegisterEventCallback(...)` 只保存回调，不调用业务 handler。
4. `Subscribe(...)` 委托客户端连接池流程。
5. `UnSubScribe(...)` 执行本地清理优先流程。
6. `Dispose()` 停止服务端、取消全部订阅、关闭全部连接、清空回调。

### 3.2 `WebSocketCommunicationModule.Host.cs`

职责：

1. 实现 `GetFullHost(protocol, hostname, port)`。
2. 返回 Core 使用的 Host 标识，并由连接层统一构造 v2 事件 endpoint。
3. 封装 CFET 固定双端口映射策略：HTTP 资源端口为配置端口，WebSocket 事件端口为配置端口 + 1。

建议规则：

```text
ws   -> ws://{hostname}:{port}
http -> ws://{hostname}:{port + WebSocketPortOffset}
其他 -> string.Empty
```

注意：

1. 返回值是 Core Host 标识，不包含 `/cfet2/event/v2`。
2. 真实 WebSocket 连接 URI 由 `RemoteEndpointKey.ToUri()` 统一拼接 `/cfet2/event/v2`。
3. 用户订阅可以写 `http://host:port/resource/path`；`EventFilter` 解析后应得到 `filter.Host = ws://host:{port + WebSocketPortOffset}`，`Source = /resource/path`。
4. CFET 默认部署占用两个相邻端口，WebSocketPortOffset 默认 1：`http` 为资源访问端口，`ws` 为事件端口。事件通信模块只负责把 `http` 写法规范化为对应的 `ws` 事件 Host。

### 3.3 `WebSocketCommunicationModule.Events.cs`

职责：

1. 封装远程业务事件回调。
2. 封装连接状态事件回调。
3. 保证 `Dispose()` 后不再调用回调。

建议方法：

```csharp
private void RaiseRemoteEvent(RemoteConnection connection, EventArg eventArg);
private void RaiseConnectionStatus(RemoteEndpointKey endpoint, string eventType, string reason, int retryCount);
```

处理要求：

1. 远程业务事件的 `Host` 必须设置为连接的 normalized host。
2. `eventArg.Sample != null` 时设置 `eventArg.Sample.IsRemote = true`。
3. 状态事件使用 `ConnectionStatusEvent.Create(...)` 创建。
4. 回调异常不应杀死 receive loop。可以记录异常，但不能直接影响连接状态机。

## 4. 配置对象

### 4.1 `WebSocketCommunicationOptions`

建议字段：

```csharp
public Uri? ListenUri { get; set; }
public bool EnableServer { get; set; }
public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);
public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(10);
public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);
public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);
public TimeSpan HeartbeatTimeout { get; set; } = TimeSpan.FromSeconds(90);
public TimeSpan IdleReleaseDelay { get; set; } = TimeSpan.FromSeconds(10);
public TimeSpan ResubscribeInterval { get; set; } = TimeSpan.FromMilliseconds(50);
public int ReceiveBufferSize { get; set; } = 64 * 1024;
public int MaxMessageBytes { get; set; } = 4 * 1024 * 1024;
public bool ReleaseConnectionImmediatelyWhenIdle { get; set; }
public WebSocketAuthOptions Auth { get; set; } = new();
```

### 4.2 `WebSocketAuthOptions`

第一版允许匿名，但保留入口：

```csharp
public bool Enabled { get; set; }
public string? Scheme { get; set; }
public string? Token { get; set; }
```

后续可替换为 `IWebSocketAuthenticator`，支持 header、query 和 hello auth。

## 5. 连接与订阅模型

### 5.1 `RemoteEndpointKey`

职责：作为连接池 key，避免字符串大小写、path 和尾部斜杠造成重复连接。

```csharp
public readonly record struct RemoteEndpointKey(
    string Scheme,
    string Host,
    int Port,
    string Path)
{
    public string NormalizedHost { get; }
    public Uri ToUri();
    public static RemoteEndpointKey Parse(string normalizedHost);
}
```

约束：

1. `Path` 必须是 `/cfet2/event/v2`。
2. `Host` 建议统一小写。
3. `NormalizedHost` 必须等价于 `EventHub.NormalizeHost(filter.Host)`。

### 5.2 `RemoteConnection`

职责：表示一个远程 endpoint 的客户端连接及其状态。

建议字段：

```csharp
public RemoteEndpointKey Endpoint { get; }
public ClientWebSocket? WebSocket { get; set; }
public ConcurrentDictionary<Guid, EventFilter> Filters { get; } = new();
public RemoteConnectionState State { get; set; }
public CancellationTokenSource LifetimeCts { get; }
public Task? ReceiveLoopTask { get; set; }
public Task? ReconnectTask { get; set; }
public Task? HeartbeatTask { get; set; }
public Timer? IdleReleaseTimer { get; set; }
public SemaphoreSlim SendLock { get; } = new(1, 1);
public SemaphoreSlim ConnectLock { get; } = new(1, 1);
public DateTime LastReceivedAtUtc { get; set; }
public int RetryCount { get; set; }
```

注意：

1. `SendLock` 只保护 WebSocket 发送。
2. `ConnectLock` 保护连接创建、握手、关闭和 idle release 竞态。
3. 状态切换应集中在少量方法中，避免任意位置直接改状态。

### 5.3 `SubscriptionEntry`

职责：保存 token 与连接的映射。

```csharp
public sealed class SubscriptionEntry
{
    public Guid TokenId { get; init; }
    public RemoteEndpointKey Endpoint { get; init; }
    public EventFilter Filter { get; init; }
    public SubscriptionState State { get; set; }
    public DateTime CreatedAtUtc { get; init; }
    public DateTime? ActivatedAtUtc { get; set; }
    public string? LastError { get; set; }
}
```

`SubscriptionState` 建议：

```csharp
public enum SubscriptionState
{
    Pending,
    Active,
    Cancelling,
    Failed
}
```

## 6. 协议 DTO

所有 DTO 放在 `Protocol` 目录下，统一由 `WebSocketMessageSerializer` 序列化。

### 6.1 常量

```csharp
public static class WebSocketProtocolConstants
{
    public const string EndpointPath = "/cfet2/event/v2";
    public const string SubProtocol = "cfet2-event.v2";
    public const string Protocol = "cfet2.websocket.event";
    public const int Version = 2;
}
```

### 6.2 Envelope 基类

```csharp
public abstract class WebSocketMessageEnvelope
{
    public string MessageType { get; init; } = "";
    public string Protocol { get; init; } = WebSocketProtocolConstants.Protocol;
    public int Version { get; init; } = WebSocketProtocolConstants.Version;
}
```

收到消息后先按 envelope 轻量解析并校验：

1. `messageType` 不为空。
2. `protocol == cfet2.websocket.event`。
3. `version == 2`。
4. 不满足时返回 `error` 或关闭连接，并产生状态事件。

### 6.3 消息类型

建议 DTO：

1. `HelloMessage`
2. `HelloAckMessage`
3. `SubscribeMessage`
4. `SubscribeAckMessage`
5. `UnsubscribeMessage`
6. `RemoteEventMessage`
7. `PingMessage`
8. `PongMessage`
9. `ErrorMessage`

`RemoteEventMessage` 中 `TokenId` 为可空字段，客户端不得依赖它完成分发；无 token 时按广播式远程事件回流 EventHub。协议不再定义 `TokenIds` 数组。

### 6.4 序列化器

`WebSocketMessageSerializer` 职责：

1. 使用 Newtonsoft.Json。
2. 提供 `SerializeToUtf8Bytes(...)`。
3. 提供 `DeserializeEnvelope(...)` 和按 `messageType` 分发的 `DeserializeMessage(...)`。
4. 处理坏 JSON、大报文和缺少 envelope 的错误。

完整 EventArg JSON 和 PerformanceLevel = 1 精简载荷均已实现；精简载荷包含 Source、EventType、Value、Timestamp，组内按最小等级选择远端载荷。

## 7. 客户端核心流程

### 7.1 Subscribe

流程：

```text
Subscribe(token, filter)
  -> 校验 token/filter/filter.Host/SourcesAndTypes
  -> normalizedHost = EventHub.NormalizeHost(filter.Host)
  -> endpoint = RemoteEndpointKey.Parse(normalizedHost)
  -> connection = connections.GetOrAdd(endpoint)
  -> connection.Filters[token.Id] = clonedFilter
  -> subscriptions[token.Id] = Pending entry
  -> 取消 idle release timer
  -> EnsureConnectedAsync(connection) 后台执行
  -> 如果已 Connected，发送单条 subscribe
```

实现原则：

1. `Subscribe(...)` 是同步接口，不应长时间阻塞业务线程。
2. 连接未完成时，订阅保持 `Pending`。
3. 连接完成后通过 flush 统一发送 `Pending/Active` 订阅。
4. 同 endpoint 并发订阅只能触发一个真实连接。

### 7.2 连接和握手

`EnsureConnectedAsync(RemoteConnection connection)`：

1. 获取 `ConnectLock`。
2. 若已 `Connected` 直接返回。
3. 创建 `ClientWebSocket`，添加 subprotocol `cfet2-event.v2`。
4. 使用 `ConnectTimeout` 连接 `/cfet2/event/v2`。
5. 发送 `hello`。
6. 在 `HandshakeTimeout` 内等待 `helloAck`。
7. 校验协议名、版本、accepted 和 heartbeat 配置。
8. 启动 receive loop、heartbeat loop。
9. 状态进入 `Connected`，发送 `ConnectionStatusEvent.Connected`。
10. 调用 `FlushSubscriptionsAsync(connection)`。

握手失败：

1. 状态进入 `Failed`。
2. 不发送真实 `subscribe`。
3. 产生 `ConnectionStatusEvent.Failed`。
4. 根据是否仍有订阅决定进入重连或释放。

### 7.3 Flush 订阅

`FlushSubscriptionsAsync(connection)`：

1. 获取当前连接仍存在的 token 列表。
2. 逐条检查 `subscriptions` 中 entry 是否仍存在。
3. 发送 `SubscribeMessage`。
4. 等待或由 receive loop 处理 `SubscribeAck`。
5. 相邻 Subscribe 之间等待 `options.ResubscribeInterval`，等待可取消。

注意：V1.0 协议只定义单条 Subscribe，不做批量消息。

### 7.4 UnSubScribe

流程：

```text
UnSubScribe(token)
  -> subscriptions.TryRemove(token.Id)
  -> 找到 connection 后移除 connection.Filters[token.Id]
  -> 如果 socket 打开，后台尽力发送 unsubscribe
  -> 如果发送失败，不回滚本地清理
  -> 如果 connection.Filters.Count == 0，执行释放策略
```

实现原则：

1. 幂等：token 不存在直接返回。
2. 本地清理优先。
3. `unsubscribe` 是 Fire-and-Forget，不等待 ack。
4. 取消一个 token 不影响同连接其他 token。

### 7.5 Receive Loop

`ReceiveLoopAsync(connection)` 职责：

1. 拼接完整 WebSocket text message。
2. 限制 `MaxMessageBytes`。
3. 反序列化 envelope。
4. 刷新 `LastReceivedAtUtc`。
5. 按 `messageType` 分发：
   - `subscribeAck`：更新订阅状态。
   - `event`：转换为 `EventArg` 并调用 `RaiseRemoteEvent(...)`。
   - `pong`：刷新心跳时间。
   - `ping`：回复 `pong`。
   - `error`：转换为订阅失败或连接失败。
6. 非主动关闭导致 loop 结束时触发 `Disconnected/Reconnecting`。

坏消息策略：

1. 缺少 envelope：拒绝，必要时关闭连接。
2. 单条坏 JSON：记录并丢弃；若属于协议阶段关键消息，则连接失败。
3. 超大报文：丢弃并记录，不允许导致进程退出。

### 7.6 Heartbeat Loop

职责：

1. 按协商间隔发送 `ping`。
2. 发送前检查连接状态。
3. 若 `UtcNow - LastReceivedAtUtc > HeartbeatTimeout`，主动关闭连接并进入重连。
4. 任意业务消息、ack、error、pong 都刷新 `LastReceivedAtUtc`。

### 7.7 重连

触发条件：

1. receive loop 意外结束。
2. heartbeat timeout。
3. 发送失败且连接上仍有订阅。

流程：

```text
Disconnected
  -> RaiseConnectionStatus(disconnected)
  -> Reconnecting
  -> 按 ReconnectPolicy 延迟
  -> 重新 Connect + Hello
  -> Connected
  -> FlushSubscriptions
```

要求：

1. 重连期间新增订阅进入 `Pending`。
2. 重连期间取消的 token 不得重发。
3. 最后一个订阅取消后停止重连并释放连接。
4. 重连成功但重订阅过程中再次断线，状态机回到 `Disconnected/Reconnecting`。

## 8. 服务端核心流程

服务端已采用 `WebSocketEventServer`、`WebSocketSession`、`WebSocketSubscriptionManager` 三层结构。

### 8.1 `WebSocketEventServer`

职责：

1. 监听 `options.ListenUri` 对应的 `/cfet2/event/v2`。
2. 校验 subprotocol `cfet2-event.v2`。
3. 接受连接后创建 `WebSocketSession`。
4. Dispose 时关闭全部 session。

实现技术选择：

1. 若项目允许引入 ASP.NET Core，可用 minimal hosting 和 WebSocket middleware。
2. 若希望减少依赖，可用 `HttpListener` 接收 WebSocket upgrade。
3. 第一版优先选工程中更容易测试、释放端口更稳定的方案。

### 8.2 `WebSocketSession`

职责：

1. 完成 hello/helloAck。
2. 维护当前 session 的远端 token 集合。
3. 收到 `subscribe` 后把远程 filter 转为本地 filter，并清空 `Host`。
4. 调用 `WebSocketSubscriptionManager` 建立聚合订阅。
5. 收到 `unsubscribe` 后只取消指定 token。
6. 本地事件触发后发送 `event` envelope。
7. session 关闭时取消全部本地订阅。
8. 心跳超时后剔除僵尸 session。

### 8.3 服务端 Subscribe 转换

远程 filter 转本地 filter 时：

1. 复制 `SourcesAndTypes`。
2. 清空 `Host`。
3. 保留 `PerformanceLevel`。
4. 服务端聚合项按 `source + eventType` 复用本地订阅，不暴露给远程客户端。
5. 聚合项保存实际释放对象，可能是 `ResourceSubscription`，也可能是普通 `Token`。

服务端建立本地订阅时不再直接持有 `EventHub`，而是持有 `Hub`：

1. 先尝试 `Hub.SubscribeResource(source, pushToRemote, eventType)`。
2. 如果 source 能解析成本地 Status/Config 资源，则建立 `ResourceSubscription`。
3. 带 `[Cfet2Event]` 的资源不进入 Core 内部观测，只接收 Thing 主动发布。
4. 未带 `[Cfet2Event]` 的资源进入 Core 内部观测，变化后发布 `changed`。
5. 如果 source 不存在，则回退为普通事件订阅 `Hub.Subscribe(localFilter, pushToRemote)`。
6. 如果 source 存在但不是可订阅资源，或未标记资源订阅非 `changed` 事件，则拒绝订阅，不回退普通事件。

## 9. 状态事件设计

所有连接状态变化通过 `RegisterEventCallback` 回流给 Core：

```csharp
ConnectionStatusEvent.Create(
    endpoint.NormalizedHost,
    endpoint.Scheme,
    ConnectionStatusEvent.Reconnecting,
    reason,
    retryCount);
```

建议触发点：

1. 首次连接并握手成功：`Connected`。
2. receive loop 意外结束：`Disconnected`。
3. 每次重连开始：`Reconnecting`。
4. 版本、鉴权、握手或长期重试失败：`Failed`。
5. 重连成功：`Connected`。

主动取消最后订阅后的正常关闭通常不需要上报 `Failed`；是否上报 `Disconnected` 可通过配置决定，默认建议不上报，避免业务层把正常释放误判为故障。

## 10. 并发与资源释放原则

1. `Subscribe`、`UnSubScribe`、`Dispose` 必须可并发调用。
2. 连接创建、握手、关闭统一走 `ConnectLock`。
3. WebSocket 发送统一走 `SendLock`。
4. `Dispose` 先标记 disposed，再停止服务端，再取消连接 CTS，最后关闭 socket 和 timer。
5. `Dispose` 后不再调用 event callback。
6. idle release timer 回调中必须二次检查 `Filters.Count == 0`。
7. 任何后台 task 的异常都要被观察，不能产生未观察异常。

## 11. 第一阶段落地拆分

建议按以下小步提交：

### 阶段 1：外壳和 Host

1. 拆分 `WebSocketCommunicationModule` partial 文件。
2. 增加 `WebSocketCommunicationOptions`。
3. 修正 `ProtocolNames` 返回 `["ws", "http"]`，并让 `GetFullHost(...)` 对 http 返回 `ws://host:{port + WebSocketPortOffset}` 的 Core Host 标识，偏移默认 1；连接 URI 再附加 `/cfet2/event/v2`。
4. 实现 `RegisterEventCallback` 和 `Dispose` 基本幂等。
5. 覆盖 Core 注册、Start 幂等、Host 解析测试。

### 阶段 2：协议 DTO 和序列化

1. 增加协议常量。
2. 增加 hello、subscribe、unsubscribe、event、ping/pong、error DTO。
3. 增加 envelope 校验。
4. 覆盖缺少 `messageType/protocol/version`、版本错误、旧裸消息拒绝测试。

### 阶段 3：客户端连接池和握手

1. 增加 `RemoteEndpointKey`、`RemoteConnection`、状态枚举。
2. 实现 `EnsureConnectedAsync`。
3. 实现 hello/helloAck 版本校验。
4. 覆盖单 endpoint 单连接、多 endpoint 多连接、旧服务端握手失败测试。

### 阶段 4：订阅和取消订阅

1. 增加 `SubscriptionEntry`。
2. 实现 Pending/Active。
3. 实现 subscribe 发送和 subscribeAck 处理。
4. 实现 UnSubScribe 本地清理优先和 fire-and-forget。
5. 覆盖并发 Subscribe、重复 UnSubScribe、取消单 token 不影响其他 token。

### 阶段 5：事件回流和状态事件

1. 实现 receive loop。
2. 实现 remote event 到 `RegisterEventCallback`。
3. 设置 `EventArg.Host` 和 `Sample.IsRemote`。
4. 实现 `ConnectionStatusEvent` 回流。
5. 覆盖业务 handler 由 EventHub 二次分发、tokenId 可选、状态事件测试。

### 阶段 6：心跳、重连和释放

1. 实现 ping/pong。
2. 实现 heartbeat timeout。
3. 实现重连退避和逐条重订阅。
4. 实现 idle release 和 graceful close。
5. 覆盖半开连接、断线重连、重连期间新增/取消 token、最后订阅释放竞态。

### 阶段 7：服务端能力

1. 增加 `WebSocketEventServer` 和 `WebSocketSession`。
2. 实现服务端 hello、subscribe、unsubscribe、event push。
3. 实现服务端 session 心跳和僵尸连接剔除。
4. 覆盖同 session 多订阅、session 关闭清理、本地 EventHub 订阅释放。

## 12. 第一版最小可交付范围

若需要先尽快形成可运行闭环，最小范围建议为：

1. GetFullHost 返回不含 endpoint path 的 Core Host 标识；连接 URI 由 RemoteEndpointKey 附加 v2 endpoint。
2. 客户端连接池。
3. hello/helloAck 版本校验。
4. subscribe/unsubscribe。
5. receive loop 处理 `event` 和 `subscribeAck`。
6. `RegisterEventCallback` 事件回流。
7. 基础断线重连。
8. 基础测试覆盖连接池、握手隔离、订阅生命周期和事件回流。

服务端能力、鉴权、复杂重连退避、idle release 竞态和大报文保护可以紧随其后完成，但 V1.0 验收前必须补齐。

## 13. 不建议第一版实现的内容

1. 不实现断线事件缓存、cursor 补偿、ack 重放。
2. 不复用旧 `EventRequest` 裸消息。
3. 不让客户端根据 `tokenId` 直接调用业务 handler。
4. 不默认连接旧根路径 `/`。
5. 不在日志中输出鉴权 token。
6. 不把 `"http"` 当作事件传输协议；它只是 CFET URI 写法入口，真实事件连接必须使用转换后的 `ws://host:{port + WebSocketPortOffset}/cfet2/event/v2`。
