# WebSocketCommunicationModule PerformanceLevel 实现设计

## 1. 文档目的

本文定义 WebSocketCommunicationModule 对 `PerformanceLevel` 的正式语义、聚合规则、协议载荷、状态更新流程和测试要求。

本文只讨论 `PerformanceLevel`。当前版本统一使用 JSON Text 传输协议消息。

## 2. 核心设计结论

`PerformanceLevel` 仅用于决定远程网络传输时发送多少事件数据，不属于事件匹配条件。

最终职责划分如下：

```text
Core / EventHub
  - 始终发布和处理完整 EventArg
  - 本地订阅统一使用 PerformanceLevel = 0
  - 不使用 PerformanceLevel 过滤事件

Subscribe 端客户端聚合
  - 按 endpoint + source + eventType 聚合
  - 不把 PerformanceLevel 放入客户端聚合键
  - 单独记录组内每个本地 token 请求的 PerformanceLevel
  - 根据组内订阅计算唯一的有效 PerformanceLevel
  - 使用带 Revision 的 SubscribeMessage upsert 更新远程订阅

Publish 端服务端聚合
  - 按 source + eventType 聚合本地 EventHub 订阅
  - 同一 source + eventType 只创建一个本地完整事件订阅
  - 每个远程 subscriber 单独记录 PerformanceLevel
  - 每个远程 subscriber 单独记录最新 Revision
  - 仅 PerformanceLevel 变化时原地更新 subscriber
  - fan-out 时按 subscriber 的 PerformanceLevel 选择网络载荷
```

## 3. PerformanceLevel 语义

第一版支持以下等级：

| 等级 | 含义 |
| --- | --- |
| `0` | 完整事件模式，要求发送完整 `EventArg` 和完整 `Sample.Context` |
| `1` | 轻量事件模式，只要求事件路由字段和 `Sample.ObjectVal` |

等级兼容关系：

```text
Level 0 订阅者只能使用完整事件。
Level 1 订阅者允许接收轻量事件，也允许接收完整事件。
```

因此，同一客户端聚合组中只要存在一个 Level 0 订阅，整个远程聚合订阅就必须使用 Level 0。

## 4. Core 与 EventHub 行为

### 4.1 Core 始终使用完整事件

本地事件发布进入 EventHub 时，必须保留完整 `EventArg` 和完整 `Sample`：

```text
EventHub.Publish(full EventArg)
```

WebSocket 服务端向本地 EventHub 注册订阅时，统一使用：

```text
PerformanceLevel = 0
```

原因是本地调用没有网络传输开销，不需要为了轻量传输提前裁剪数据。

### 4.2 Core 不按 PerformanceLevel 过滤

`EventFilter.Predicate(...)` 继续只根据 `Source` 和 `EventType` 判断是否匹配。

不应增加以下过滤条件：

```text
filter.PerformanceLevel == eventArg.PerformanceLevel
```

远程事件被客户端接收后，仍按照现有 Core 事件路由规则进行本地广播。

## 5. 客户端订阅聚合

### 5.1 客户端聚合键

客户端聚合键继续保持：

```text
endpoint + normalized source + normalized eventType
```

`PerformanceLevel` 不进入 `ClientSubscriptionGroupKey`。

原因是同一客户端对同一个远程事件只应建立一个远程订阅，并只接收一次远程事件。

### 5.2 聚合组需要记录每个本地订阅的等级

当前聚合组只记录本地 token 是否存在：

```text
LocalTokenId -> 存在
```

支持 `PerformanceLevel` 后，需要记录：

```text
LocalTokenId -> RequestedPerformanceLevel
```

概念结构如下：

```text
ClientSubscriptionGroup
  MasterTokenId
  Revision
  Key(endpoint, source, eventType)
  LocalSubscriptions
    tokenA -> Level 0
    tokenB -> Level 1
    tokenC -> Level 1
  EffectivePerformanceLevel = 0
```

### 5.3 有效等级计算

组内有效等级采用“满足所有订阅者要求”的规则：

```text
存在任意 Level 0 -> EffectivePerformanceLevel = 0
全部为 Level 1 -> EffectivePerformanceLevel = 1
```

对于当前只支持 `0` 和 `1` 的设计，可以理解为取组内最小值：

```text
EffectivePerformanceLevel = Min(RequestedPerformanceLevels)
```

### 5.4 新增本地订阅

新增订阅后需要重新计算有效等级。

场景一：

```text
原有：[Level 1, Level 1]
新增：Level 1
结果：有效等级仍为 Level 1
远程动作：无
```

场景二：

```text
原有：[Level 1, Level 1]
新增：Level 0
结果：有效等级从 Level 1 变为 Level 0
远程动作：更新远程订阅
```

### 5.5 取消本地订阅

取消订阅后需要重新计算有效等级。

场景一：

```text
取消后组内仍存在 Level 0
结果：有效等级仍为 Level 0
远程动作：无
```

场景二：

```text
取消最后一个 Level 0，剩余订阅全部为 Level 1
结果：有效等级从 Level 0 变为 Level 1
远程动作：更新远程订阅
```

场景三：

```text
取消组内最后一个本地订阅
远程动作：unsubscribe
删除客户端聚合组
```

### 5.6 同一本地 token 更新等级

同一个本地 token 使用相同规则重新订阅，但 `PerformanceLevel` 发生变化时，不能直接视为幂等。

必须更新该 token 在聚合组中记录的等级，并重新计算有效等级：

```text
tokenA: Level 1 -> Level 0
```

如果有效等级发生变化，则更新远程订阅；如果有效等级未变化，则不发送网络动作。

### 5.7 更新远程订阅

不增加 `UpdateSubscriptionMessage`。

`SubscribeMessage` 定义为远程订阅完整状态的 upsert：

```text
同一 session + 同一 TokenId + 更高 Revision
  -> 更新远程订阅

同一 session + 同一 TokenId + 相同 Revision
  -> 幂等重复请求
```

有效等级变化时，客户端保留现有 `MasterTokenId`，递增 `Revision`，并再次发送完整 `SubscribeMessage`：

```text
SubscribeMessage
  TokenId = existing MasterTokenId
  Revision = previous Revision + 1
  Filter.Source/EventType = existing rule
  Filter.PerformanceLevel = new EffectivePerformanceLevel
```

`SubscribeAckMessage` 必须回传对应 Revision：

```text
SubscribeAckMessage
  TokenId
  Revision
  Accepted
  Reason
```

客户端只处理与当前订阅条目 Revision 相同的 ACK：

```text
ACK.Revision == CurrentRevision
  -> 更新当前订阅状态

ACK.Revision < CurrentRevision
  -> 过期 ACK，忽略

ACK.Revision > CurrentRevision
  -> 协议状态异常，不得覆盖当前状态
```

客户端订阅存储需要同时保存：

```text
DesiredRevision
LastAcceptedRevision
CurrentFilter
State
```

客户端必须在同一个订阅存储临界区内，原子更新远程 Filter、有效 PerformanceLevel 和 Revision。发送失败或断线后，重连只发送最新 Filter 和最新 Revision。

### 5.8 重连与重新订阅

客户端订阅存储中应保存聚合组当前的有效等级和最新 Revision。

重连成功后重新发送订阅时，必须使用最新有效等级和最新 Revision，而不是首次创建聚合组时的状态。

重连属于新 WebSocket session。服务端不能依赖旧 session 中保存的 Revision，但客户端仍应发送当前 Revision，便于同一新 session 内处理重试和后续更新。

## 6. 服务端订阅聚合

### 6.1 服务端本地聚合键

服务端用于管理本地 EventHub 订阅的聚合键应为：

```text
source + eventType
```

`PerformanceLevel` 不进入本地聚合键。

因此，同一个 `Source/EventType` 无论远程客户端请求什么等级，都只创建一个本地 EventHub 订阅。

### 6.2 本地 EventHub 订阅

服务端创建本地订阅时统一使用完整事件模式：

```text
EventFilter
  Source = remote requested source
  EventType = remote requested eventType
  Host = empty
  PerformanceLevel = 0
```

本地 EventHub 回调始终提供完整 `EventArg`。

### 6.3 RemoteSubscriber 记录 PerformanceLevel

每个远程订阅者需要单独记录等级：

```text
RemoteSubscriber
  SessionId
  RemoteTokenId
  Session
  PerformanceLevel
  Revision
```

聚合结果示例：

```text
ServerSubscriptionKey(/thing/status, Changed)
  Local EventHub Token: one
  Subscribers:
    sessionA/tokenA -> Level 0
    sessionB/tokenB -> Level 1
    sessionC/tokenC -> Level 1
```

### 6.4 服务端重复订阅

服务端以 `(SessionId, RemoteTokenId)` 识别一个远程订阅，并保存其最新 Revision。

收到 `SubscribeMessage` 后按照 Revision 处理：

```text
远程 token 不存在
  -> 创建 subscriber，保存 Revision

Revision < 当前 Revision
  -> 过期请求，不得覆盖新状态，返回携带请求 Revision 的 staleRevision 拒绝 ACK

Revision == 当前 Revision，且 Filter 与当前状态一致
  -> 幂等重复请求，返回该 Revision 已保存的相同 ACK 结果

Revision == 当前 Revision，但 Filter 与当前状态不一致
  -> 协议冲突，拒绝请求

Revision > 当前 Revision
  -> 更新远程订阅
```

服务端需要为当前 Revision 保存最近一次处理结果，至少包括 `Accepted` 和 `Reason`，用于相同 Revision 请求的幂等响应。

服务端更新远程订阅时，根据变化内容选择处理方式。

#### 6.4.1 仅 PerformanceLevel 变化

如果 Source/EventType 集合不变，仅有效 `PerformanceLevel` 变化：

1. 不移除 subscriber。
2. 不释放或重新创建本地 EventHub token。
3. 原地更新 `RemoteSubscriber.PerformanceLevel`。
4. 原地更新 `RemoteSubscriber.Revision`。
5. 返回包含新 Revision 的成功 ACK。

由于服务端本地聚合键不包含 `PerformanceLevel`，仅等级变化不需要迁移聚合项。

#### 6.4.2 Source/EventType 发生变化

如果 Source/EventType 集合变化：

1. 从旧聚合项中移除该 subscriber。
2. 清理旧的反向索引。
3. 按新的 Source/EventType 重新加入聚合项，并在 subscriber 上保存新的 PerformanceLevel。
4. 保存新的 Revision。
5. 如果旧聚合项已无 subscriber，则释放对应本地 EventHub token。

更新过程必须在服务端订阅管理器锁内完成，避免 fan-out 或并发更新看到部分迁移状态。

## 7. 服务端事件 Fan-out

### 7.1 基本流程

本地 EventHub 发布一次完整事件后，服务端只收到一次回调：

```text
完整 EventArg
  -> 获取当前聚合项 subscriber 快照
  -> 按 subscriber.PerformanceLevel 选择远程载荷
  -> 向各 session 发送
```

### 7.2 Level 0 载荷

Level 0 必须发送完整事件，至少包括：

```text
TokenId
PerformanceLevel = 0
Host
Source
EventType
完整 Sample.Context
```

应保持当前完整事件行为兼容。

### 7.3 Level 1 载荷

Level 1 只发送：

```text
TokenId
PerformanceLevel = 1
Source
EventType
Value = EventArg.Sample.ObjectVal
```

默认不发送：

```text
完整 Sample.Context
错误列表
Path
ResourceType
IsRemote
其他上下文字段
```

### 7.4 避免重复转换

一次本地事件 fan-out 中，可以分别构造一次 Level 0 和 Level 1 载荷数据：

```text
full payload：按需构造一次
lightweight payload：按需构造一次
```

由于每个远程订阅者的 `TokenId` 不同，建议缓存不包含 `TokenId` 的事件载荷，再在发送时包装对应 token。

不得修改 Core 提供的原始 `EventArg`，否则可能影响其他本地订阅者或其他远程发送等级。

## 8. 客户端接收与事件重建

### 8.1 Level 0

客户端按照当前完整事件逻辑反序列化，并设置：

```text
EventArg.PerformanceLevel = 0
EventArg.Host = 当前远程 endpoint
EventArg.Sample.IsRemote = true
```

### 8.2 Level 1

客户端根据轻量载荷重建：

```text
EventArg
  PerformanceLevel = 1
  Host = 当前远程 endpoint
  Source = payload.Source
  EventType = payload.EventType
  Sample = new Status<object>(payload.Value)
  Sample.IsRemote = true
```

重建后的 Level 1 事件交给 Core 正常广播。Core 不根据等级过滤。

由于客户端只有在组内全部订阅都接受 Level 1 时才请求 Level 1，因此本地订阅者不会收到无法满足其要求的轻量事件。

## 9. 协议与 DTO

### 9.1 序列化格式

当前版本统一使用 JSON 序列化，并通过 `WebSocketMessageType.Text` 发送协议消息。

`PerformanceLevel = 0` 和 `PerformanceLevel = 1` 均使用 JSON 表达，二者仅在远程事件载荷包含的数据量上存在差异。

### 9.2 建议的事件 DTO

完整事件 DTO：

```text
RemoteFullEventPayload
  PerformanceLevel = 0
  Host
  Source
  EventType
  SampleContext
```

轻量事件 DTO：

```text
RemoteLightweightEventPayload
  PerformanceLevel = 1
  Source
  EventType
  Value
```

网络 envelope 继续携带远程 `TokenId`。

### 9.3 Subscribe Upsert 与 Revision

不增加 `UpdateSubscriptionMessage`。订阅创建、完整状态更新、重试和重连恢复统一使用 `SubscribeMessage`。

建议协议字段：

```text
SubscribeMessage
  TokenId
  Revision
  Filter

SubscribeAckMessage
  TokenId
  Revision
  Accepted
  Reason
```

Revision 规则：

- Revision 是同一 session、同一远程 token 内单调递增的非负整数。
- 首次订阅建议使用 `Revision = 1`。
- 只有期望远程订阅完整状态发生变化时才递增 Revision。
- 相同 Revision 的重试必须携带完全相同的 Filter。
- ACK 必须回传请求中的 Revision。
- 服务端不得使用较低 Revision 覆盖较高 Revision。
- 客户端不得使用过期 ACK 覆盖当前订阅状态。
- 新 session 中服务端从收到的第一条订阅开始建立 Revision 状态，不依赖旧 session。

### 9.4 不支持的等级

服务端收到不支持的等级时，应拒绝该条订阅：

```text
subscribeAck.accepted = false
subscribeAck.revision = request.revision
subscribeAck.reason = "Unsupported PerformanceLevel: {value}"
```

不得静默降级，也不得影响同一连接上的其他订阅。

客户端收到拒绝 ACK 后，将对应远程订阅标记为 `Failed`。

## 10. 并发与竞态要求

需要重点处理以下竞态：

1. 新增 Level 0 与取消最后一个 Level 0 同时发生时，最终远程等级必须与聚合组当前快照一致。
2. 有效等级变化产生的订阅更新必须与普通 subscribe/unsubscribe 保持同 endpoint 动作顺序。
3. 远程订阅更新过程中发生断线时，重连后必须发送最新有效等级。
4. 服务端 fan-out 获取 subscriber 快照后，订阅等级可能被更新；在途事件允许使用快照中的旧等级，后续事件必须使用新等级。
5. 同一 session/token 更新等级时，不得同时保留旧等级 subscriber 和新等级 subscriber。
6. 取消最后一个本地订阅后，不得因待处理的等级更新重新创建远程订阅。
7. 连续更新产生多个 ACK 时，客户端只能使用当前 Revision 的 ACK 更新状态。
8. 服务端收到过期 Revision 时，不得回滚 subscriber 的 Filter、PerformanceLevel 或 Revision。
9. 相同 Revision 的重复 Subscribe 必须幂等，且不得重复创建本地 EventHub 订阅。
10. 相同 Revision 携带不同 Filter 时必须拒绝，不能猜测请求顺序。

## 11. 配置建议

建议增加：

```text
SupportedPerformanceLevels = [0, 1]
AllowPerformanceLevelFallback = false
```

默认不允许静默降级。

如果未来允许显式降级，服务端 ACK 必须返回最终采用的等级，客户端聚合状态也必须记录实际等级。

## 12. 实施步骤

建议按以下顺序实施：

1. 明确并实现 Level 0、Level 1 远程事件 DTO。
2. 为 `SubscribeMessage` 和 `SubscribeAckMessage` 增加 Revision。
3. 修改客户端聚合组，记录每个本地 token 的请求等级和当前 Revision。
4. 实现客户端聚合组有效等级计算。
5. 支持有效等级变化时递增 Revision，并使用相同 MasterTokenId 发送 Subscribe upsert。
6. 客户端订阅存储忽略过期 ACK。
7. 修改服务端本地聚合键，移除 `PerformanceLevel`。
8. 服务端本地 EventHub 订阅统一使用 Level 0。
9. 在 `RemoteSubscriber` 中记录远程请求等级和最新 Revision。
10. 服务端实现 Revision 校验、幂等处理和仅 PerformanceLevel 变化时的原地更新。
11. 服务端 fan-out 按 subscriber 等级选择完整或轻量载荷。
12. 客户端按载荷等级重建完整或轻量 `EventArg`。
13. 增加不支持等级校验和拒绝 ACK。
14. 补充聚合、等级切换、Revision、重连和并发测试。

## 13. 验收测试

### 13.1 全部 Level 1

1. 同一客户端对相同 Source/EventType 创建多个 Level 1 本地订阅。
2. 确认只发送一个远程 subscribe。
3. 确认远程 subscribe 的等级为 Level 1。
4. 确认服务端只发送一次轻量事件。
5. 确认所有本地订阅正常收到事件。

### 13.2 Level 0 与 Level 1 混合订阅

1. 先建立 Level 1 订阅。
2. 再增加相同 Source/EventType 的 Level 0 订阅。
3. 确认客户端聚合组有效等级从 Level 1 切换到 Level 0。
4. 确认使用相同 MasterTokenId 和递增 Revision 更新远程订阅。
5. 确认服务端仅原地更新 subscriber，不重建本地 EventHub 订阅。
6. 确认服务端发送一次完整事件。
7. 确认两个本地订阅均只收到一次事件。

### 13.3 取消最后一个 Level 0

1. 聚合组中同时存在 Level 0 和 Level 1。
2. 取消最后一个 Level 0。
3. 确认有效等级切换为 Level 1。
4. 确认远程订阅更新为 Level 1。
5. 确认后续只发送轻量事件。

### 13.4 等级变化但有效等级不变

1. 聚合组中存在多个 Level 0。
2. 取消其中一个 Level 0。
3. 确认有效等级仍为 Level 0。
4. 确认不会发送多余的远程订阅更新。

### 13.5 同一本地 token 更新等级

1. token 以 Level 1 订阅。
2. 同一 token、相同规则重新以 Level 0 订阅。
3. 确认本地等级记录被更新。
4. 确认远程有效等级正确更新。

### 13.6 不同远程客户端请求不同等级

1. 客户端 A 请求 Level 0。
2. 客户端 B 请求 Level 1。
3. 确认服务端只创建一个本地 EventHub 订阅。
4. 确认 A 收到完整事件。
5. 确认 B 收到轻量事件。

### 13.7 重连恢复

1. 建立 Level 1 聚合订阅。
2. Publish 端短暂下线后恢复。
3. 确认重新订阅仍使用当前有效 Level 1。
4. 确认恢复后继续收到轻量事件。

### 13.8 不支持的等级

1. 请求服务端不支持的 PerformanceLevel。
2. 确认服务端返回拒绝 ACK 和明确原因。
3. 确认该订阅进入 Failed。
4. 确认连接和其他订阅继续正常工作。

### 13.9 连续更新与乱序 ACK

1. 同一 MasterToken 连续产生 Revision 2 和 Revision 3 更新。
2. 模拟 Revision 2 的 ACK 晚于 Revision 3 返回。
3. 确认客户端忽略 Revision 2 的过期 ACK。
4. 确认当前 Filter、PerformanceLevel 和状态仍对应 Revision 3。

### 13.10 Subscribe 幂等与旧 Revision

1. 对同一 session/token 重复发送相同 Revision 和相同 Filter。
2. 确认服务端幂等返回，不重复创建本地订阅。
3. 发送更低 Revision，确认服务端不回滚当前状态。
4. 使用相同 Revision 发送不同 Filter，确认服务端拒绝协议冲突。

### 13.11 PerformanceLevel 原地更新

1. 建立某 Source/EventType 的 Level 1 远程订阅。
2. 使用更高 Revision 和相同 Source/EventType 更新为 Level 0。
3. 确认服务端 subscriber 的 PerformanceLevel 和 Revision 已更新。
4. 确认本地 EventHub token 未被释放或重新创建。
5. 确认后续事件使用 Level 0 完整载荷。

## 14. 最终不变量

实现完成后必须始终满足：

```text
Core 永远处理完整事件。
PerformanceLevel 永远不参与 Core 事件匹配。
同一客户端的同一 endpoint/source/eventType 只存在一个远程聚合订阅。
客户端聚合组的远程等级能够满足组内所有本地订阅者。
同一服务端 Source/EventType 只存在一个本地 EventHub 订阅。
服务端根据每个远程 subscriber 的等级决定网络载荷。
SubscribeMessage 是带 Revision 的完整状态 upsert，不存在 UpdateSubscriptionMessage。
客户端和服务端都不得使用旧 Revision 覆盖新订阅状态。
仅 PerformanceLevel 变化时，服务端必须原地更新 subscriber，不重建本地 EventHub 订阅。
Level 1 事件不携带完整 Sample.Context。
不支持的等级不得静默降级。
```
