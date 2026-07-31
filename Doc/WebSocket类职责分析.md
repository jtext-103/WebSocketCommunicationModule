# WebSocket 类职责分析

本文基于当前 `WebSocketCommunicationModule` 代码重新分析 WebSocket 模块内每个主要类的功能、职责边界和剩余风险。

分析时间点：`RemoteEndpointClient` 已完成二次拆分，客户端侧已从一个大类拆成门面、动作处理器、连接状态机、消息处理器和订阅状态仓库。

## 1. 总体结论

当前 WebSocket 模块已经形成比较清晰的分层：

```text
WebSocketCommunicationModule
  -> ClientSubscriptionAggregator
  -> RemoteEndpointClient
       -> RemoteEndpointActionProcessor
       -> RemoteEndpointConnection
            -> RemoteEndpointMessageHandler
       -> RemoteEndpointSubscriptionStore

WebSocketEventServer
  -> WebSocketSession
  -> WebSocketSubscriptionManager

Protocol/*
Options/*
Connections/*
```

当前职责边界比旧版本清楚很多：

- `WebSocketCommunicationModule` 是 Core 模块门面，负责启动、订阅入口、退订入口、事件回调和 endpoint client 池。
- `ClientSubscriptionAggregator` 只做客户端本地订阅到远端聚合订阅的内存状态计算。
- `RemoteEndpointClient` 已不再直接承担 WebSocket 收发、重连、消息处理和动作队列细节，只负责单 endpoint 门面协调。
- `RemoteEndpointActionProcessor` 独立串行处理单 endpoint 的 subscribe/unsubscribe 动作。
- `RemoteEndpointConnection` 独立维护 `ClientWebSocket`、连接状态、握手、发送、接收、心跳和重连。
- `RemoteEndpointMessageHandler` 独立处理远端协议消息。
- `RemoteEndpointSubscriptionStore` 独立维护远端 master token 的订阅状态。
- 服务端侧保持 `WebSocketEventServer`、`WebSocketSession`、`WebSocketSubscriptionManager` 三层结构。

剩余主要风险：

1. 客户端和服务端仍重复实现 WebSocket 文本帧收发逻辑。
2. `RemoteEndpointConnection` 仍偏大，连接、收发、心跳、重连都在一个类中。
3. `ReconnectPolicy`、`IWebSocketDelayProvider`、`SystemDelayProvider` 已接入连接层，但目前只覆盖延迟策略和等待抽象。
4. 服务端存在少量 `Console.WriteLine`，和模块 logger 体系不一致。
5. `WebSocketSession` 同时负责握手、协议分发、订阅调用、发送远端事件和帧收发，后续可以继续拆。

## 2. 模块入口层

### 2.1 `WebSocketCommunicationModule`

位置：`WebSocketCommunicationModule/WebSocketCommunicationModule.cs`

当前功能：

- 实现 `IEventCommunicationModule`。
- 声明支持协议：`ws`、`http`。
- 管理模块生命周期：`Start`、`Dispose`。
- 根据配置启动和释放 `WebSocketEventServer`。
- 保存 Core 注册的 `eventCallback`。
- 持有 `RemoteEndpointKey -> RemoteEndpointClient` 的 endpoint client 池。
- 持有 `ClientSubscriptionAggregator`。
- `Subscribe` 时校验 token/filter，提示 `PerformanceLevel` 不支持，然后让 aggregator 生成 `ClientSubscriptionPlan`。
- `UnSubScribe` 时让 aggregator 生成退订计划。
- 通过 `EnqueueClientPlan` 把 plan 路由到对应 `RemoteEndpointClient`。
- 监听 endpoint client 的 `RemoteEventReceived`、`ConnectionStatusChanged`、`Idle` 事件。
- client idle 时二次校验 `RemoteEndpointClient.IsIdle` 和 `subscriptionAggregator.HasLocalSubscriptions(endpoint)`，然后从字典移除并释放 client。

职责评价：

- 职责边界合理，是模块级 facade。
- 不直接维护远端订阅字典，不直接操作 `ClientWebSocket`，这点已经清理得比较好。
- `EnqueueClientPlan` 属于 facade 到 endpoint actor 的路由职责，可以保留。

建议：

- 保持当前边界。
- 日志文本中 `Connent` 拼写建议修正为 `Connect` 或 `Connection`。
- `Dispose` 中释放所有 client 后清空 aggregator 的顺序合理，继续保持。

### 2.2 `WebSocketCommunicationModule.Helper`

位置：`WebSocketCommunicationModule/WebSocketCommunicationModule.Helper.cs`

当前功能：

- 把 `ClientSubscriptionPlan.Actions` 投递到 endpoint client。
- 创建 `RemoteEndpointClient` 并接线事件。
- 将 endpoint client 的远端事件转成模块级事件。
- 将连接状态事件转成 Core `ConnectionStatusEvent`。
- 处理 idle client 的移除和释放。

职责评价：

- 是 `WebSocketCommunicationModule` 的辅助 partial，职责集中在 endpoint client 协调。
- 现在没有网络细节，边界清楚。

建议：

- 可以继续保留为 facade 协调代码。
- `OnClientIdle` 的二次校验很重要，应保留，避免退订和新订阅竞争时误释放连接。

### 2.3 `WebSocketCommunicationModule.Events`

位置：`WebSocketCommunicationModule/WebSocketCommunicationModule.Events.cs`

当前功能：

- 远端事件进入 Core callback 前，设置 `EventArg.Host` 为 endpoint normalized host。
- 设置 `Sample.IsRemote = true`。
- 捕获 callback 异常，避免用户 callback 影响连接状态机。

职责评价：

- 是事件适配层，职责清晰。
- 不直接参与订阅聚合或网络收发。

建议：

- callback 异常日志可以补充异常对象或更完整原因。

### 2.4 `WebSocketCommunicationModule.Host`

位置：`WebSocketCommunicationModule/WebSocketCommunicationModule.Host.cs`

当前功能：

- 实现 `GetFullHost(protocol, hostname, port)`。
- `http` 映射到 `ws://host:{port + 1}`。
- `ws` 映射到 `ws://host:{port}`。

职责评价：

- host 映射职责清晰。
- 返回值不包含 `/cfet2/event/v2`，而 `RemoteEndpointKey.Parse` 会补齐/强制 endpoint path。功能可用，但命名里的 `FullHost` 容易让人误解为完整 endpoint。

建议：

- 明确该方法返回基础 host，或改为拼接 `WebSocketProtocolConstants.EndpointPath`。

## 3. 客户端订阅聚合层

### 3.1 `ClientSubscriptionAggregator`

位置：`WebSocketCommunicationModule/Client/ClientSubscriptionAggregator.cs`

当前功能：

- 维护客户端本地订阅聚合状态：
  - `ClientSubscriptionGroupKey -> ClientSubscriptionGroup`
  - `local token id -> LocalSubscriptionBinding`
- 校验 token、filter 和 filter host。
- 将 `EventFilter.SourcesAndTypes` 去重成多个 `ClientSubscriptionGroupKey`。
- 同一个 `(endpoint, source, eventType)` 只创建一个 master token。
- 多个本地 token 订阅同一 group 时只增加本地引用。
- 本地 token 更新订阅时计算旧 group 和新 group 的差异。
- 最后一个本地 token 取消时生成远端 unsubscribe action。
- 只生成 `ClientSubscriptionPlan`，不执行网络动作。
- 提供 `HasLocalSubscriptions(endpoint)` 供 idle 释放二次校验。

职责评价：

- 职责非常清晰，是纯内存聚合状态机。
- 不创建 socket，不连接，不发送协议消息。
- 单一 `syncRoot` 保护内部字典，锁边界可读。

潜在问题：

- `HasLocalSubscriptions(endpoint)` 通过扫描 group 实现。当前规模可接受；如果 endpoint 和 group 数量很大，可以增加 endpoint 索引。
- 输入的 `EventFilter` 应按调用约定视为不可变；如果外部在调用期间修改同一个 filter 实例，可能读到变化中的集合。

建议：

- 保持其只做状态和 plan 生成。
- 不要把网络动作、client 创建或事件回调逻辑放入该类。

### 3.2 `ClientSubscriptionPlan`

位置：`WebSocketCommunicationModule/Client/ClientSubscriptionPlan.cs`

当前功能：

- 表示 aggregator 输出的远端动作计划。
- 包含统一顺序动作列表 `Actions`。
- `IsEmpty` 根据 `Actions.Count == 0` 判断。

职责评价：

- DTO 职责清楚。
- 当前已没有 `SubscribeActions` / `UnsubscribeActions` 的重复表达，结构比旧版本更干净。

建议：

- 保持只读 plan 语义。

### 3.3 `ClientRemoteAction`

位置：`WebSocketCommunicationModule/Client/ClientSubscriptionPlan.cs`

当前功能：

- 表示 module 层需要路由的远端动作。
- 包含 `Kind`、`Endpoint`、`MasterTokenId`、可选 `RemoteFilter`。

职责评价：

- 是跨 endpoint 的 action DTO。
- 与 `RemoteEndpointAction` 语义相近，但层级不同：`ClientRemoteAction` 带 endpoint，用于 module 路由；`RemoteEndpointAction` 不带 endpoint，用于单 endpoint 队列。

建议：

- 可以保留两个类型。
- 如果要进一步命名清晰，可把 `ClientRemoteAction` 改名为 `ClientEndpointAction`。

## 4. 客户端 endpoint 层

### 4.1 `RemoteEndpointClient`

位置：`WebSocketCommunicationModule/Client/RemoteEndpointClient.cs`

当前功能：

- 表示一个远端 endpoint 的客户端门面。
- 持有：
  - `RemoteEndpointSubscriptionStore`
  - `RemoteEndpointConnection`
  - `RemoteEndpointActionProcessor`
  - idle release timer
- 对外暴露：
  - `Endpoint`
  - `State`
  - `SubscriptionCount`
  - `IsIdle`
  - `RemoteEventReceived`
  - `ConnectionStatusChanged`
  - `Idle`
- 将 `EnqueueSubscribe` / `EnqueueUnsubscribe` 委托给 action processor。
- 将 `EnsureConnectedAsync` / `SendUnsubscribeAsync` 委托给 connection。
- 对订阅查询和快照委托给 subscription store。
- 在取消订阅后根据 idle 策略触发 `Idle`。
- `Dispose` 时先释放 connection，再释放 action processor，最后清理订阅 store。

职责评价：

- 当前已是单 endpoint facade，不再直接承担 WebSocket 细节。
- idle release 留在 client 门面合理，因为它需要协调 subscription store、options 和模块的 `Idle` 事件。
- `State` 从 connection 派生，client 自身 disposed 时返回 `Disposed`，外部观察语义清楚。

关键时序：

- `Idle` 可能在 action processor 的处理循环内触发。
- `Dispose` 必须先停止 connection，再等待 action processor 停止，否则重连循环可能在退订释放时多启动一次。

建议：

- 保持当前门面职责。
- 后续如果 action processor 和 connection 的委托参数继续增多，可以引入小型接口，例如 `IRemoteEndpointClientCallbacks`。

### 4.2 `RemoteEndpointActionProcessor`

位置：`WebSocketCommunicationModule/Client/RemoteEndpointActionProcessor.cs`

当前功能：

- 独立持有 `Channel<RemoteEndpointAction>`。
- 串行消费同一 endpoint 内的 subscribe/unsubscribe 动作。
- `EnqueueSubscribe`：
  - 移除 pending unsubscribe 标记。
  - 将 subscribe action 写入队列。
- `EnqueueUnsubscribe`：
  - 如果订阅存在，先标记 pending unsubscribe。
  - 将 unsubscribe action 写入队列。
  - 写入失败时移除 pending 标记。
- `ProcessActionsAsync`：
  - subscribe：写入/更新 subscription store，然后确保连接。
  - unsubscribe：从 store 移除，发送 unsubscribe。
- `Dispose` 取消处理循环并等待最多 1 秒。

职责评价：

- 职责清晰，是单 endpoint 顺序动作处理器。
- 成功把队列和顺序保证从 `RemoteEndpointClient` 中拆出。

潜在问题：

- `ProcessActionsAsync` 只捕获取消异常。如果未来某个委托抛出未捕获异常，action loop 会结束，后续动作无法处理。
- `WaitForActionLoopToStop` 同步等待 1 秒，属于容错型释放；如果循环卡住不会强制失败。

建议：

- 在单个 action 处理内部增加通用异常保护和日志，避免消费者退出。
- 如果未来需要更可测，可以暴露内部诊断状态或注入 channel。

### 4.3 `RemoteEndpointConnection`

位置：`WebSocketCommunicationModule/Client/RemoteEndpointConnection.cs`

当前功能：

- 独立维护单 endpoint 的 `ClientWebSocket`。
- 持有连接状态 `RemoteConnectionState`。
- 管理：
  - connect lock
  - send lock
  - lifetime cancellation token
  - receive loop task
  - heartbeat task
  - reconnect task
  - connection generation
  - retry count
  - last received time
- `EnsureConnectedAsync`：
  - 避免并发连接。
  - 已连接时 flush pending subscriptions。
  - 未连接时执行连接和握手。
- `ConnectAndHandshakeAsync`：
  - 创建 `ClientWebSocket`。
  - 设置 subprotocol。
  - 设置 Authorization header。
  - 连接 endpoint。
  - 发送 `HelloMessage`。
  - 接收并校验 `HelloAckMessage`。
  - 启动 receive loop 和 heartbeat loop。
  - flush pending subscriptions。
- `FlushSubscriptionsAsync`：
  - 从 subscription store 快照 pending subscription。
  - 发送 `SubscribeMessage`。
  - 失败时回滚发送状态并标记连接失败。
- `ReceiveLoopAsync`：
  - 接收 text message。
  - 反序列化协议消息。
  - 交给 `RemoteEndpointMessageHandler` 处理。
  - 读失败时进入 disconnected 并调度重连。
- `HeartbeatLoopAsync`：
  - 定时发送 ping。
  - 检查 heartbeat timeout。
  - timeout 或发送失败时 abort socket 并调度重连。
- `ReconnectLoopAsync`：
  - 非 idle 时持续尝试重连。
  - 每次重连前把 active/pending 订阅重置为 pending。
- `SendUnsubscribeAsync`：
  - 已连接时发送 `UnsubscribeMessage`。

职责评价：

- 作为单 endpoint 连接状态机，职责是合理的。
- 该类仍是客户端侧最复杂的类，原因是连接生命周期、心跳和重连天然耦合。
- 已经不再处理业务事件和订阅 ACK 细节，这部分交给 message handler 和 store。

潜在问题：

- 重连延迟已由 `ReconnectPolicy` 计算，默认仍保持 50ms 线性增长、最大 1000ms。
- 客户端和服务端重复实现 text frame send/receive。
- `receiveLoopTask`、`heartbeatTask` 字段仅保存引用，不直接等待或诊断。

建议：

- 后续可抽 `WebSocketTextTransport` 统一 send/receive text frame。
- 后续可继续扩展 `ReconnectPolicy`，支持指数退避、jitter 或最大重试次数。
- 若要继续拆，可拆出 `RemoteEndpointHeartbeat` 和 `RemoteEndpointReconnectLoop`，但要小心 generation、state 和 lifetime token 的时序。

### 4.4 `RemoteEndpointMessageHandler`

位置：`WebSocketCommunicationModule/Client/RemoteEndpointMessageHandler.cs`

当前功能：

- 处理客户端收到的协议消息：
  - `SubscribeAckMessage`：更新 subscription store。
  - `RemoteEventMessage`：校验 eventArg 和 token 后回调给 client。
  - `ErrorMessage`：更新订阅错误状态。
  - `PingMessage`：当前 generation 有效时发送 `PongMessage`。
  - `PongMessage`：刷新 last received 时间。

职责评价：

- 职责清晰，是协议消息分发器。
- 不维护连接状态，不负责重连，不直接操作 socket 生命周期。

建议：

- 保持现状。
- 如果协议消息类型继续增加，可以把 switch 拆成小 handler 方法。

### 4.5 `RemoteEndpointSubscriptionStore`

位置：`WebSocketCommunicationModule/Client/RemoteEndpointSubscriptionStore.cs`

当前功能：

- 维护 master token 到 `RemoteSubscriptionEntry` 的字典。
- 维护 `pendingUnsubscribeTokenIds`。
- 提供：
  - `AddOrUpdate`
  - `Remove`
  - `Get`
  - `Snapshot`
  - `Contains`
  - `TryMarkSubscribeSending`
  - `MarkSubscribeSendFailed`
  - `MarkSubscribeAck`
  - `MarkError`
  - `ResetForReconnect`
  - `MarkActiveOrPendingFailed`
  - `IsIdle`
  - `IsEffectivelyIdle`
- 用锁保护订阅字典和 pending unsubscribe 集合。

职责评价：

- 职责非常清晰，是远端订阅状态仓库。
- `IsEffectivelyIdle` 对重连停止非常关键：当所有剩余订阅都已经 pending unsubscribe 时，不应继续重连。

建议：

- 保持状态变更集中在该类。
- 若未来订阅状态变复杂，可以把状态转换规则显式化。

### 4.6 `RemoteEndpointActionKind` / `RemoteEndpointAction`

位置：`WebSocketCommunicationModule/Client/RemoteEndpointAction.cs`

当前功能：

- `RemoteEndpointActionKind` 定义 endpoint 内部动作类型：`Subscribe`、`Unsubscribe`。
- `RemoteEndpointAction` 是单 endpoint action queue 的消息。

职责评价：

- 简单 DTO/enum，职责清晰。
- 不携带 endpoint 是正确的，因为队列已属于某个 endpoint。

### 4.7 `RemoteSubscriptionEntry`

位置：`WebSocketCommunicationModule/Client/RemoteSubscriptionEntry.cs`

当前功能：

- 保存一个 master token 的远端订阅状态。
- 包含 token、endpoint、filter、状态、创建时间、激活时间、发送时间、最后错误。

职责评价：

- 状态模型职责清楚。
- 当前为 `public sealed class`，但主要由内部 client 使用。

建议：

- 如果外部没有直接依赖，可以考虑改为 `internal`。

### 4.8 `ConnectionStatusEventArgs`

位置：`WebSocketCommunicationModule/Client/RemoteEndpointClientEvents.cs`

当前功能：

- endpoint client 连接状态变化事件参数。
- 包含 `EventType`、`Reason`、`RetryCount`。

职责评价：

- DTO 职责清楚。

### 4.9 `RemoteEndpointBackgroundTask`

位置：`WebSocketCommunicationModule/Client/RemoteEndpointBackgroundTask.cs`

当前功能：

- 观察后台 task 的 faulted exception，避免未观察异常。

职责评价：

- 小型内部工具类，职责清楚。

建议：

- 后续可增加 logger 参数，在吞掉异常前记录日志。

## 5. 连接与订阅模型层

### 5.1 `RemoteEndpointKey`

位置：`WebSocketCommunicationModule/Connections/RemoteEndpointKey.cs`

当前功能：

- 表示远端 endpoint 值对象。
- 规范化 scheme、host、port、path。
- 强制 path 为 `/cfet2/event/v2`。
- 提供 `ToUri()` 和 `NormalizedHost`。

职责评价：

- 值对象职责清晰。
- 依赖 `EventHub.NormalizeHost` 是 CFET 集成语义，可以接受。

### 5.2 `ClientSubscriptionGroupKey`

位置：`WebSocketCommunicationModule/Connections/ClientSubscriptionGroupKey.cs`

当前功能：

- 客户端聚合订阅 key。
- 按 `Endpoint + Source + EventType` 聚合。

职责评价：

- 值对象职责清晰。
- 不包含 `PerformanceLevel`，与当前 WebSocketCM 不支持性能等级传输优化的实现一致。

### 5.3 `ClientSubscriptionGroup`

位置：`WebSocketCommunicationModule/Connections/ClientSubscriptionGroup.cs`

当前功能：

- 表示一个真实远端聚合订阅。
- 保存 master token、group key、remote filter、创建时间。
- 保存引用该 group 的本地 token 集合。

职责评价：

- 状态容器职责清晰。

### 5.4 `LocalSubscriptionBinding`

位置：`WebSocketCommunicationModule/Connections/LocalSubscriptionBinding.cs`

当前功能：

- 记录本地 token 当前对应的 group keys 和 master token ids。
- 支持本地 token 更新订阅时计算 diff。

职责评价：

- 状态容器职责清楚。

### 5.5 `RemoteConnectionState`

位置：`WebSocketCommunicationModule/Connections/RemoteConnectionState.cs`

当前功能：

- 定义远端连接状态：`Disconnected`、`Connecting`、`Connected`、`Reconnecting`、`Failed`、`Disposed`。

职责评价：

- enum 职责清楚。

### 5.6 `SubscriptionState`

位置：`WebSocketCommunicationModule/Connections/SubscriptionState.cs`

当前功能：

- 定义订阅状态：`Pending`、`Active`、`Cancelling`、`Failed`。

职责评价：

- enum 职责清楚。
- `Cancelling` 当前未被主要流程使用，更像预留状态。

### 5.7 `ReconnectPolicy`

位置：`WebSocketCommunicationModule/Connections/ReconnectPolicy.cs`

当前功能：

- 计算重连等待时间。
- 默认 `InitialDelay = 50ms`，`MaxDelay = 1s`。
- `GetDelay(retryCount)` 按 retry 次数线性增长，并受最大延迟限制。
- 当 `InitialDelay` 或 `MaxDelay` 小于等于 0 时返回 `TimeSpan.Zero`。

职责评价：

- 已接入 `RemoteEndpointConnection.ReconnectLoopAsync`。
- 当前策略简单直接，保持旧行为兼容。

建议：

- 后续可增加指数退避、jitter、最大重试次数等策略字段。

### 5.8 `IWebSocketDelayProvider` / `SystemDelayProvider`

位置：

- `WebSocketCommunicationModule/Connections/IWebSocketDelayProvider.cs`
- `WebSocketCommunicationModule/Connections/SystemDelayProvider.cs`

当前功能：

- 抽象异步等待能力。
- `SystemDelayProvider` 是生产默认实现。
- `RemoteEndpointConnection` 在重订阅间隔、心跳间隔和重连间隔中使用该接口。

职责评价：

- 已接入连接层。
- 测试可以注入 recording/fake delay provider，确认重连路径使用指定策略，也可以减少真实等待。

建议：

- 如果未来需要更快更稳定的时序测试，可以进一步使用 fake delay provider 替换更多真实等待。

## 6. 服务端层

### 6.1 `WebSocketEventServer`

位置：`WebSocketCommunicationModule/Server/WebSocketEventServer.cs`

当前功能：

- 管理 Kestrel WebSocket 服务端生命周期。
- 根据 `WebSocketCommunicationOptions.ListenUri` 建立监听。
- 调用 `UseWebSockets()`。
- 只映射 `WebSocketProtocolConstants.EndpointPath`。
- 校验 WebSocket request 和 subprotocol。
- 接受 WebSocket 后创建 `WebSocketSession`。
- 持有 session 字典，session 结束后移除。
- 持有 `WebSocketSubscriptionManager`。

职责评价：

- 是服务端监听和 session 管理器，职责清晰。
- 不直接处理 subscribe/unsubscribe 业务细节，这点合理。

潜在问题：

- 使用 `Console.WriteLine` 输出服务端状态和 subprotocol 错误。
- `eventHub` 字段只用于构造 subscription manager，之后未直接使用。

建议：

- 替换 `Console.WriteLine` 为 logger。
- 可移除未使用字段或保留为语义说明。

### 6.2 `WebSocketSession`

位置：`WebSocketCommunicationModule/Server/WebSocketSession.cs`

当前功能：

- 表示一个远端 WebSocket 客户端会话。
- 维护当前 session 的 remote token 集合。
- 执行 hello/helloAck 握手。
- 循环接收协议消息。
- 处理：
  - `SubscribeMessage`
  - `UnsubscribeMessage`
  - `PingMessage`
  - `PongMessage`
- 调用 `WebSocketSubscriptionManager` 建立或取消本地订阅。
- 发送 `SubscribeAckMessage`。
- 向远端发送 `RemoteEventMessage`。
- 释放时取消本 session 的所有订阅。
- 串行化发送。
- 实现 WebSocket text frame 接收。

职责评价：

- 当前是服务端单连接 actor。
- 职责比客户端 message handler 更集中，仍同时承担协议分发、订阅动作和帧收发。
- 在当前规模下可接受，但后续服务端复杂度上升时应拆分。

潜在问题：

- 与 `RemoteEndpointConnection` 重复实现 `SendAsync` / `ReceiveTextMessageAsync`。
- `HandshakeAsync` 使用 `Console.WriteLine`。
- `UnsubscribeAsync` 不发送 ack，这是当前协议设计可接受，但文档应明确。

建议：

- 抽共享 text transport。
- 拆出服务端消息 handler，例如 `WebSocketSessionMessageHandler`。
- 用 logger 替换 console。

### 6.3 `WebSocketSubscriptionManager`

位置：`WebSocketCommunicationModule/Server/Subscriptions/WebSocketSubscriptionManager.cs`

当前功能：

- 聚合服务端收到的远端订阅。
- 维护：
  - `ServerSubscriptionKey -> AggregatedServerSubscription`
  - `RemoteSubscriptionKey -> ServerSubscriptionKey[]`
- 多个远端 subscriber 订阅同一服务端 key 时共用一个本地 EventHub token。
- 本地 EventHub 事件触发后 fan out 到所有远端 subscriber。
- 支持单 token 退订、session 全量退订和 dispose 清理。
- 使用 `maxFanOutConcurrency` 限制并发推送。

职责评价：

- 职责清晰，是服务端订阅聚合状态机。
- 不持有 WebSocket listener，不解析协议消息，只调用 session 发送事件。

潜在问题：

- fan out 回调中如果 session 发送失败，当前主要依赖 session 内部吞掉异常。
- 复杂状态由两个字典维护，需要继续保持所有增删路径的一致性。

建议：

- 保持聚合逻辑集中在该类。
- 可增加针对 session dispose 与 fan out 并发的测试覆盖。

### 6.4 `AggregatedServerSubscription`

位置：`WebSocketCommunicationModule/Server/Subscriptions/AggregatedServerSubscription.cs`

当前功能：

- 表示服务端侧一个聚合后的本地 EventHub 订阅。
- 保存本地 token、本地 filter 和远端 subscriber 集合。

职责评价：

- 状态容器职责清楚。

### 6.5 `RemoteSubscriber`

位置：`WebSocketCommunicationModule/Server/Subscriptions/RemoteSubscriber.cs`

当前功能：

- 表示一个远端订阅者。
- 包含 session id、remote token id 和 `WebSocketSession` 引用。

职责评价：

- 值/引用容器职责清楚。

### 6.6 `RemoteSubscriptionKey`

位置：`WebSocketCommunicationModule/Server/Subscriptions/RemoteSubscriptionKey.cs`

当前功能：

- 表示远端订阅 key：session id + remote token id。

职责评价：

- 值对象职责清楚。

### 6.7 `ServerSubscriptionKey`

位置：`WebSocketCommunicationModule/Server/Subscriptions/ServerSubscriptionKey.cs`

当前功能：

- 表示服务端聚合 key：source + event type。

职责评价：

- 值对象职责清楚。

## 7. 协议层

### 7.1 `WebSocketProtocolConstants`

位置：`WebSocketCommunicationModule/Protocol/WebSocketProtocolConstants.cs`

当前功能：

- 定义 endpoint path、subprotocol、协议名、版本和消息类型常量。

职责评价：

- 常量集中管理，职责清楚。

### 7.2 `WebSocketMessageEnvelope`

位置：`WebSocketCommunicationModule/Protocol/WebSocketMessageEnvelope.cs`

当前功能：

- 所有协议消息的基类。
- 包含 `MessageType`、`Protocol`、`Version`。

职责评价：

- 协议 envelope 模型职责清楚。

### 7.3 协议消息 DTO

位置：`WebSocketCommunicationModule/Protocol/*.cs`

包括：

- `HelloMessage`
- `HelloAckMessage`
- `SubscribeMessage`
- `SubscribeAckMessage`
- `UnsubscribeMessage`
- `RemoteEventMessage`
- `ErrorMessage`
- `PingMessage`
- `PongMessage`
- `HeartbeatMessage`

当前功能：

- 分别表达握手、订阅、取消订阅、远端事件、错误和心跳消息。

职责评价：

- DTO 职责清晰。
- `HeartbeatMessage` 当前主流程未直接使用，实际心跳使用 `PingMessage` / `PongMessage`。

建议：

- 如果不需要兼容旧协议，可以评估删除或明确 `HeartbeatMessage` 为预留消息。

### 7.4 `WebSocketMessageSerializer`

位置：`WebSocketCommunicationModule/Protocol/WebSocketMessageSerializer.cs`

当前功能：

- 序列化 `WebSocketMessageEnvelope`。
- 反序列化 envelope。
- 根据 `messageType` 转换成具体消息类型。
- 校验协议名和版本。
- 特殊处理 `RemoteEventMessage` 中的 `EventArg` / `Sample`。
- 支持最大消息大小限制。

职责评价：

- 协议序列化职责清晰。
- 包含 `EventArg` / `Sample` 反序列化细节，属于协议兼容所需。

潜在问题：

- 类型分发 switch 会随协议消息增加而变长。
- sample 反序列化逻辑和 Core 模型耦合较深。

建议：

- 当前可保留。
- 如果协议继续扩展，可引入 message registry 或小型 mapper。

## 8. 配置与认证层

### 8.1 `WebSocketCommunicationOptions`

位置：`WebSocketCommunicationModule/Options/WebSocketCommunicationOptions.cs`

当前功能：

- 定义监听地址、是否启用服务端。
- 定义连接、发送、握手、心跳、idle release、重订阅间隔。
- 定义接收缓冲和最大消息大小。
- 定义服务端 fan out 并发度。
- 定义 idle 时是否立即释放连接。
- 持有 `WebSocketAuthOptions`。

职责评价：

- 配置对象职责清楚。
- 客户端和服务端共用一个 options，当前可接受。

建议：

- 如果配置继续增多，可拆成 client/server 子配置。

### 8.2 `WebSocketAuthOptions`

位置：`WebSocketCommunicationModule/Options/WebSocketAuthOptions.cs`

当前功能：

- 定义认证开关、scheme、token。

职责评价：

- 配置 DTO 职责清楚。
- 当前客户端会把配置转成 Authorization header 和 hello 字段。
- 服务端当前尚未实际鉴权。

建议：

- 如果要启用认证，应接入 `WebSocketSession.HandshakeAsync`。

### 8.3 `IWebSocketAuthenticator` / `AllowAnonymousWebSocketAuthenticator`

位置：`WebSocketCommunicationModule/Auth/*.cs`

当前功能：

- 预留 WebSocket 认证扩展点。
- 当前匿名认证实现直接允许。

职责评价：

- 属于预留扩展点，主流程未使用。

建议：

- 要么接入服务端握手流程，要么暂时删除以减少未使用概念。

## 9. 当前主要流程

### 9.1 客户端订阅流程

```text
WebSocketCommunicationModule.Subscribe
  -> ClientSubscriptionAggregator.Subscribe
  -> ClientSubscriptionPlan.Actions
  -> WebSocketCommunicationModule.EnqueueClientPlan
  -> RemoteEndpointClient.EnqueueSubscribe
  -> RemoteEndpointActionProcessor.EnqueueSubscribe
  -> ProcessActionsAsync
  -> RemoteEndpointClient.AddOrUpdateSubscription
  -> RemoteEndpointConnection.EnsureConnectedAsync
  -> ConnectAndHandshakeAsync
  -> FlushSubscriptionsAsync
  -> SubscribeMessage
```

### 9.2 客户端退订流程

```text
WebSocketCommunicationModule.UnSubScribe
  -> ClientSubscriptionAggregator.Unsubscribe
  -> ClientSubscriptionPlan.Actions
  -> RemoteEndpointClient.EnqueueUnsubscribe
  -> RemoteEndpointActionProcessor.EnqueueUnsubscribe
  -> ProcessActionsAsync
  -> RemoteEndpointClient.RemoveSubscription
  -> RemoteEndpointConnection.SendUnsubscribeAsync
  -> RemoteEndpointClient.ScheduleIdleReleaseIfStillIdle
  -> Idle event
  -> WebSocketCommunicationModule.OnClientIdle
  -> Remove endpoint client and Dispose
```

### 9.3 客户端接收远端事件流程

```text
RemoteEndpointConnection.ReceiveLoopAsync
  -> WebSocketMessageSerializer.DeserializeMessage
  -> RemoteEndpointMessageHandler.HandleIncomingMessage
  -> RemoteEndpointSubscriptionStore.Contains
  -> RemoteEndpointClient.RemoteEventReceived
  -> WebSocketCommunicationModule.OnRemoteEventReceived
  -> WebSocketCommunicationModule.RaiseRemoteEvent
  -> Core callback
```

### 9.4 服务端接收远端订阅流程

```text
WebSocketEventServer.HandleContextAsync
  -> AcceptWebSocketAsync
  -> WebSocketSession.RunAsync
  -> HandshakeAsync
  -> Receive SubscribeMessage
  -> WebSocketSubscriptionManager.Subscribe
  -> local EventHub subscription
  -> SubscribeAckMessage
```

### 9.5 服务端推送本地事件到远端流程

```text
EventHub callback
  -> WebSocketSubscriptionManager.FanOutAsync
  -> RemoteSubscriber.Session.SendRemoteEventAsync
  -> RemoteEventMessage
```

## 10. 建议的下一步优化

优先级从高到低：

1. 抽共享 WebSocket text transport。
   - 客户端 `RemoteEndpointConnection` 和服务端 `WebSocketSession` 都有 send/receive text frame 逻辑。
   - 可统一处理 buffer、max message bytes、text-only 校验和 close message。

2. 扩展已接入的重连和延迟抽象。
   - `ReconnectPolicy` 可增加指数退避、jitter、最大重试次数。
   - `IWebSocketDelayProvider` 可配合 fake time 改善时序测试速度和稳定性。
   - `IWebSocketAuthenticator`
   - `AllowAnonymousWebSocketAuthenticator`

3. 增强 `RemoteEndpointActionProcessor` 的异常保护。
   - 单个 action 异常不应终止整个 action loop。

4. 统一 logger。
   - 替换服务端 `Console.WriteLine`。
   - 后台任务异常观察可记录日志。

5. 拆服务端 session。
   - 可拆出 `WebSocketSessionMessageHandler`。
   - 可复用共享 transport。

6. 根据需求拆 client/server options。
   - 当前共用 options 仍可接受。
   - 如果认证、重连、服务端限制继续增加，拆配置会更清楚。

## 11. 总结

当前代码的核心职责拆分已经到位。客户端侧不再由 `RemoteEndpointClient` 独自承担所有行为，而是形成：

```text
RemoteEndpointClient              endpoint 门面
RemoteEndpointActionProcessor     动作串行化
RemoteEndpointConnection          连接状态机
RemoteEndpointMessageHandler      协议消息处理
RemoteEndpointSubscriptionStore   订阅状态仓库
```

服务端侧仍是：

```text
WebSocketEventServer              服务监听和 session 生命周期
WebSocketSession                  单连接会话 actor
WebSocketSubscriptionManager      服务端订阅聚合
```

整体看，当前最大的问题不再是“类职责混在一起”，而是少量共用基础能力尚未抽象，尤其是 WebSocket 文本帧 transport、重连策略和认证扩展点。只要后续优化继续保持“门面负责协调，状态机负责状态，handler 负责消息，store 负责数据”的边界，模块复杂度会保持在可控范围内。
