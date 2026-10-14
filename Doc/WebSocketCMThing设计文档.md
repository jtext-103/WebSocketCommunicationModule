# WebSocketCMThing 状态暴露设计

## 实现状态（2026-09-21）

公开诊断 Thing 和 DTO 已实现。本文保留设计依据及验收清单；清单不是本次测试执行记录。当前 `status.State` 为 Created/Started/Disposed，连接和订阅健康度须结合失败计数、Pending 计数和明细判断。APP 默认挂载路径为 `/WebSocketCM`，自定义宿主可自行命名。

内部诊断已采用 `GetDiagnosticStatus()`、`GetSessionStatuses()` 和计数属性；下文内部扩展建议不应作为现有函数签名直接调用。

## 1. 文档目的

本文定义 `WebSocketCMThing` 的职责、对外 Status 接口、状态 DTO、快照采集方式和测试要求。

`WebSocketCMThing` 用于对外暴露 `WebSocketCommunicationModule` 的运行状态，主要回答以下问题：

1. WebSocketCM 是否已启动，服务端是否正在监听。
2. 当前有多少远程客户端连接到本机 WebSocket 服务端。
3. 本机作为客户端连接了哪些远端 WebSocket 服务端，各连接当前是什么状态。
4. 客户端侧和服务端侧分别存在多少订阅，是否有失败或积压。
5. 出现问题时，可以定位到具体 endpoint、session 或订阅状态。

第一版只提供只读 Status，不提供断开连接、清理订阅、强制重连等写操作。

## 2. 核心设计结论

### 2.1 Thing 是薄适配层

`WebSocketCMThing` 只负责把模块诊断接口映射为 CFET2 Status：

```text
WebSocketCMThing
  -> WebSocketCommunicationModule.GetStatus()
  -> WebSocketCommunicationModule.PrintStatus()
  -> WebSocketCommunicationModule.GetServerSessions()
  -> WebSocketCommunicationModule.PrintServerSessions()
  -> WebSocketCommunicationModule.GetRemoteClients()
  -> WebSocketCommunicationModule.PrintRemoteClients()
  -> WebSocketCommunicationModule.GetRemoteClient(hostname)
  -> WebSocketCommunicationModule.PrintRemoteClient(hostname)
```

状态采集逻辑应放在状态所属对象中，不允许 `WebSocketCMThing` 通过反射读取私有字段，也不允许直接持有内部并发字典。

### 2.2 所有接口返回只读快照

每次调用 Status 时构造新的 DTO 快照。返回后，DTO 内容不随内部连接状态继续变化。

快照允许是“弱一致”的：不同连接可能来自略有时间差的瞬时状态，但必须满足：

- 不抛出集合并发修改异常。
- 不暴露可修改的内部对象。
- 单个 DTO 内的字段尽量由同一次所属对象快照生成。
- 查询不能长时间持有连接生命周期锁，也不能等待网络 IO。

### 2.3 明确区分客户端侧和服务端侧

WebSocketCM 同时具有两个角色：

| 角色 | 当前内部对象 | 对外含义 |
| --- | --- | --- |
| 本机作为客户端连接远端 | `RemoteEndpointClient` | `RemoteClients` |
| 远程客户端连接本机服务端 | `WebSocketSession` | `ServerSessions` |

“当前连接客户端数量”默认指连接到本机服务端的活动 `WebSocketSession` 数量，即 `ServerActiveSessionCount`。

本机主动连接远端的连接数量使用 `RemoteClientCount` 和 `RemoteConnectedClientCount` 表达，避免混淆。

### 2.4 连接数和订阅数必须区分

同一条物理 WebSocket 连接可以承载多个订阅，多个本地订阅也可能聚合成一个远程订阅。

因此状态中至少区分：

- 物理 WebSocket 连接数。
- 远程协议订阅数。
- 客户端聚合组数。
- 本地订阅 token 数。
- 服务端聚合后的 EventHub 本地订阅数。

## 3. Thing 定义

建议将 Thing 放在 WebSocketCommunicationModule 项目中：

```text
WebSocketCommunicationModule/
  Diagnostics/
    WebSocketCMThing.cs
    WebSocketModuleStatus.cs
    WebSocketRemoteClientStatus.cs
    WebSocketRemoteSubscriptionStatus.cs
    WebSocketServerSessionStatus.cs
```

Thing 定义：

```csharp
public sealed class WebSocketCMThing : Thing
{
    private readonly WebSocketCommunicationModule module;

    public WebSocketCMThing(WebSocketCommunicationModule module)
    {
        this.module = module ?? throw new ArgumentNullException(nameof(module));
    }

    [Cfet2Status("status")]
    public WebSocketModuleStatus GetStatus()
        => module.GetStatus();

    [Cfet2Status("printStatus")]
    public string PrintStatus()
        => module.PrintStatus();

    [Cfet2Status("serverSessions")]
    public IReadOnlyList<WebSocketServerSessionStatus> GetServerSessions()
        => module.GetServerSessions();

    [Cfet2Status("printServerSessions")]
    public string PrintServerSessions()
        => module.PrintServerSessions();

    [Cfet2Status("remoteClients")]
    public IReadOnlyList<WebSocketRemoteClientStatus> GetRemoteClients()
        => module.GetRemoteClients();

    [Cfet2Status("printRemoteClients")]
    public string PrintRemoteClients()
        => module.PrintRemoteClients();

    [Cfet2Status("remoteClient")]
    public WebSocketRemoteClientStatus? GetRemoteClient(string hostname)
        => module.GetRemoteClient(hostname);

    [Cfet2Status("printRemoteClient")]
    public string PrintRemoteClient(string hostname)
        => module.PrintRemoteClient(hostname);
}
```

应用层负责将同一个模块实例注入 Thing 并挂载：

```csharp
var webSocketCM = new WebSocketCommunicationModule(options);
MyHub.TryAddCommunicationModule(webSocketCM);
MyHub.TryAddThing(new WebSocketCMThing(webSocketCM), "/", "websocketcm");
```

不允许 `WebSocketCMThing` 自己创建另一个 `WebSocketCommunicationModule` 实例。

## 4. 对外 Status 接口

### 4.1 `status`

用途：低成本查看模块总体运行状态和关键计数，适合首页、健康检查和轮询。

接口：

```csharp
[Cfet2Status("status")]
public WebSocketModuleStatus GetStatus();
```

建议返回：

```csharp
public sealed class WebSocketModuleStatus
{
    public DateTime SnapshotAtUtc { get; init; }

    public string State { get; init; } = string.Empty;
    public bool Started { get; init; }
    public bool Disposed { get; init; }

    public bool ServerEnabled { get; init; }
    public bool ServerStarted { get; init; }
    public string? ListenUri { get; init; }

    public int ServerActiveSessionCount { get; init; }
    public int ServerRemoteSubscriptionCount { get; init; }
    public int ServerAggregatedSubscriptionCount { get; init; }
    public int ActiveFanOutEventCount { get; init; }

    public int RemoteClientCount { get; init; }
    public int RemoteConnectedClientCount { get; init; }
    public int RemoteFailedClientCount { get; init; }
    public int RemoteSubscriptionCount { get; init; }
    public int RemotePendingSubscriptionCount { get; init; }
    public int RemoteActiveSubscriptionCount { get; init; }
    public int RemoteFailedSubscriptionCount { get; init; }

    public int ClientAggregationGroupCount { get; init; }
    public int LocalSubscriptionCount { get; init; }
}
```

`State` 第一版定义：

| 值 | 条件 |
| --- | --- |
| `Created` | 模块尚未 `Start()` |
| `Started` | 模块已启动；不代表所有连接或订阅已就绪 |
| `Disposed` | 模块已释放 |

说明：

- `ServerActiveSessionCount`：当前 `sessions` 中仍由服务端管理的 session 数量。
- `ServerRemoteSubscriptionCount`：所有 session 的远程 token 去重数，键为 `(SessionId, RemoteTokenId)`。
- `ServerAggregatedSubscriptionCount`：服务端向本地 EventHub 建立的聚合订阅数。
- `RemoteClientCount`：当前 `endpointClients` 数量，包含连接中、重连中和失败的 endpoint client。
- `RemoteConnectedClientCount`：状态为 `Connected` 的 `RemoteEndpointClient` 数量。
- `RemoteSubscriptionCount`：客户端侧发往远端的 master token 数。
- `RemotePendingSubscriptionCount`：各远端客户端 Pending 订阅数量之和；与 Active/Failed 计数一起判断订阅进度。
- `ClientAggregationGroupCount`：客户端侧按 `endpoint + source + eventType` 聚合后的组数。
- `LocalSubscriptionCount`：进入 WebSocketCM 的本地 token 数；一个 token 包含多条规则时仍计为一个。

`status` 不返回 endpoint、session 和 token 明细，保证接口轻量。

### 4.2 `serverSessions`

用途：查看哪些远程客户端正在连接本机服务端，以及各 session 当前订阅数量。

接口：

```csharp
[Cfet2Status("serverSessions")]
public IReadOnlyList<WebSocketServerSessionStatus> GetServerSessions();
```

建议返回：

```csharp
public sealed class WebSocketServerSessionStatus
{
    public Guid SessionId { get; init; }
    public string State { get; init; } = string.Empty;
    public string? RemoteAddress { get; init; }

    public DateTime CreatedAtUtc { get; init; }
    public DateTime? HandshakeCompletedAtUtc { get; init; }
    public DateTime LastReceivedAtUtc { get; init; }

    public int RemoteSubscriptionCount { get; init; }
}
```

服务端 session 状态定义：

| 状态 | 含义 |
| --- | --- |
| `Open` 等 `WebSocketState` 字符串 | 当前底层 socket 状态；通过 `HandshakeCompletedAtUtc` 判断 CFET2 握手是否完成 |
| `Disposed` | 已释放；通常不会继续出现在活动列表中 |

实现要求：

- `RemoteAddress` 从 `HttpContext.Connection.RemoteIpAddress` 和 `RemotePort` 获取。
- 不暴露认证 header、认证 token 或完整 hello 内容。
- `LastReceivedAtUtc` 在收到完整协议消息后更新。
- session 明细按 `CreatedAtUtc` 升序或 `SessionId` 稳定排序，方便比较两次结果。

当前 `WebSocketSession.GetDiagnosticStatus()` 已返回状态、远端地址、创建时间、握手完成时间和最后接收时间；释放后的快照状态为 `Disposed`，其余使用 `socket.State.ToString()`。

### 4.3 `remoteClients`

用途：查看本机作为客户端连接远端服务端的连接状态和订阅明细。

接口：

```csharp
[Cfet2Status("remoteClients")]
public IReadOnlyList<WebSocketRemoteClientStatus> GetRemoteClients();
```

建议返回：

```csharp
public sealed class WebSocketRemoteClientStatus
{
    public string Endpoint { get; init; } = string.Empty;
    public string State { get; init; } = string.Empty;

    public DateTime? ConnectedAtUtc { get; init; }
    public DateTime? LastReceivedAtUtc { get; init; }
    public int RetryCount { get; init; }
    public string? LastError { get; init; }

    public int ActionQueueLength { get; init; }
    public int SubscriptionCount { get; init; }
    public int PendingSubscriptionCount { get; init; }
    public int ActiveSubscriptionCount { get; init; }
    public int FailedSubscriptionCount { get; init; }

    public IReadOnlyList<WebSocketRemoteSubscriptionStatus> Subscriptions { get; init; }
        = Array.Empty<WebSocketRemoteSubscriptionStatus>();
}
```

订阅明细：

```csharp
public sealed class WebSocketRemoteSubscriptionStatus
{
    public Guid TokenId { get; init; }
    public string Source { get; init; } = string.Empty;
    public string EventType { get; init; } = string.Empty;
    public int PerformanceLevel { get; init; }

    public int Revision { get; init; }
    public int? LastAcceptedRevision { get; init; }
    public string State { get; init; } = string.Empty;

    public DateTime CreatedAtUtc { get; init; }
    public DateTime? ActivatedAtUtc { get; init; }
    public DateTime? SubscribeSentAtUtc { get; init; }
    public int SubscribeRetryCount { get; init; }
    public string? LastError { get; init; }
}
```

说明：

- `Endpoint` 使用 normalized WebSocket endpoint，例如 `ws://127.0.0.1:8003/cfet2/event/v2`。
- `State` 直接映射现有 `RemoteConnectionState`。
- 第一版客户端聚合后，每个 master token 对应单条 `Source + EventType`，DTO 仍应在快照转换时显式读取并校验。
- `LastError` 当前直接映射内部错误；512 字符截断属于后续增强建议，不能假定已实现。
- 当前 `Subscriptions` 按 `Source`、`EventType` 排序。

当前 `RemoteEndpointClient.GetDiagnosticStatus()` 已汇总连接时间、错误、动作队列长度以及订阅明细与状态计数。

### 4.4 `remoteClient`

用途：按 hostname 查询本机连接某个远端 WebSocket 服务端的状态。

接口：

```csharp
[Cfet2Status("remoteClient")]
public WebSocketRemoteClientStatus? GetRemoteClient(string hostname);
```

`hostname` 支持以下格式：

```text
http://ip:port
ws://ip:port
```

归一化规则与 WebSocketCM 现有 `GetFullHost(...)` 和 `RemoteEndpointKey.Parse(...)` 保持一致：

| 输入 | 实际查询 endpoint |
| --- | --- |
| `http://ip:port` | `ws://ip:{port + WebSocketPortOffset}/cfet2/event/v2` |
| `ws://ip:port` | `ws://ip:port/cfet2/event/v2` |

要求：

- scheme 和 hostname 大小写不敏感。
- 允许输入末尾 `/`，归一化后再查询。
- 第一版不接受缺少 scheme 或 port 的字符串。
- hostname 为 null、空白或解析抛出 FormatException 时返回 `null`。
- endpoint 当前不存在时返回 `null`，不得为了查询而创建连接。
- 返回 DTO 结构与 `remoteClients` 中的单项完全一致，并包含订阅明细。

### 4.5 Print 接口

用途：为命令行、日志查看和人工排障提供可直接阅读的文本状态。

接口定义：

```csharp
[Cfet2Status("printStatus")]
public string PrintStatus();

[Cfet2Status("printServerSessions")]
public string PrintServerSessions();

[Cfet2Status("printRemoteClients")]
public string PrintRemoteClients();

[Cfet2Status("printRemoteClient")]
public string PrintRemoteClient(string hostname);
```

Print 接口与结构化接口对应关系：

| Print 接口 | 数据来源 |
| --- | --- |
| `PrintStatus()` | `GetStatus()` |
| `PrintServerSessions()` | `GetServerSessions()` |
| `PrintRemoteClients()` | `GetRemoteClients()` |
| `PrintRemoteClient(hostname)` | `GetRemoteClient(hostname)` |

Print 接口要求：

- 只负责格式化对应 Get 接口返回的快照，不得重新读取或修改内部通信状态。
- 输出使用多行纯文本，字段顺序固定，方便人工阅读和测试。
- 时间统一使用 UTC ISO 8601 格式。
- 当前 `PrintServerSessions()` 和 `PrintRemoteClients()` 对空列表返回空字符串。
- `PrintRemoteClient(hostname)` 未找到 endpoint 时返回空字符串。
- `PrintRemoteClient(hostname)` 的 hostname 校验和归一化规则与 `GetRemoteClient(hostname)` 完全一致。
- Print 接口不得包含认证信息等敏感字段。

Print 接口定位为方便人工使用的辅助接口。程序调用、监控采集和跨版本集成应优先使用结构化 Get 接口。

## 5. 模块内部诊断接口

`WebSocketCommunicationModule` 增加以下公共只读接口，供 Thing 或应用内诊断直接使用：

```csharp
public WebSocketModuleStatus GetStatus();

public string PrintStatus();

public IReadOnlyList<WebSocketServerSessionStatus> GetServerSessions();

public string PrintServerSessions();

public IReadOnlyList<WebSocketRemoteClientStatus> GetRemoteClients();

public string PrintRemoteClients();

public WebSocketRemoteClientStatus? GetRemoteClient(string hostname);

public string PrintRemoteClient(string hostname);
```

建议状态采集调用链：

```text
WebSocketCommunicationModule.GetStatus
  -> GetRemoteClients() -> RemoteEndpointClient.GetDiagnosticStatus()
  -> WebSocketEventServer 的计数属性
  -> ClientSubscriptionAggregator 的计数属性

WebSocketCommunicationModule.GetRemoteClients / GetRemoteClient
  -> RemoteEndpointClient.GetDiagnosticStatus()

WebSocketCommunicationModule.GetServerSessions
  -> WebSocketEventServer.GetSessionStatuses()
  -> WebSocketSession.GetDiagnosticStatus()

Print 接口基于对应的 DTO 查询结果生成文本。
```

内部诊断对象和计数按当前实现读取，不会为查询主动创建连接或重试订阅。公开 DTO 使用字符串状态。

## 6. 各对象的诊断职责与扩展建议

### 6.1 `WebSocketCommunicationModule`

需要汇总：

- `started`、`disposed`。
- `endpointClients` 快照。
- `server` 快照。
- `subscriptionAggregator` 快照。

读取 `endpointClients` 时先执行 `Values.ToArray()`，之后再逐个读取 client 状态，避免长时间持有 `lifecycleLock`。

### 6.2 `ClientSubscriptionAggregator`

需要在 `syncRoot` 内一次性返回：

- `ClientAggregationGroupCount`。
- `LocalSubscriptionCount`。

不能把 `clientSubscriptionGroups` 或 `localSubscriptionIndex` 返回给调用方。

### 6.3 `RemoteEndpointClient`

需要聚合：

- endpoint、连接状态。
- action queue 长度。
- 连接诊断字段。
- 订阅快照及各状态计数。

### 6.4 `RemoteEndpointConnection`

需要增加线程安全读取的诊断字段：

- `ConnectedAtUtc`。
- `LastReceivedAtUtc`。
- `RetryCount`。
- `LastError`。

状态切换和错误处理时同步更新这些字段。诊断读取不得返回 `ClientWebSocket` 实例。

### 6.5 `RemoteEndpointActionProcessor`

暴露只读 `QueuedActionCount`，使用 `Volatile.Read(ref queuedActionCount)`。

### 6.6 `WebSocketEventServer`

需要返回：

- `IsStarted`。
- `ListenUri`。
- 活动 session 快照。
- subscription manager 汇总。

`admittedSessionCount` 是并发准入控制计数，不建议直接作为对外活动连接数。活动连接数以 `sessions.Count` 为准。

### 6.7 `WebSocketSession`

需要增加 session 生命周期和时间字段，并提供快照方法。

session 从 `sessions` 删除后不再出现在 `serverSessions` 中，因此外部通常看不到 `Disposed` session。保留 `Disposed` 状态是为了保证快照竞态下语义完整。

### 6.8 `WebSocketSubscriptionManager`

在 `syncRoot` 内汇总：

- `RemoteSubscriptionCount = remoteIndex.Count`。
- `AggregatedSubscriptionCount = aggregatedSubscriptions.Count`。
- `ActiveFanOutEventCount`。

第一版不对外返回所有服务端远程订阅明细。原因是服务端订阅量可能较大，且明细中包含业务资源路径；需要时再单独增加分页或按 `SessionId` 查询接口。

## 7. 安全与性能约束

### 7.1 不暴露敏感信息

禁止暴露：

- `Authorization` header。
- `AuthToken`。
- 原始 hello 消息。
- WebSocket 对象。
- 内部锁、CTS、Task 等实现对象。

`RemoteAddress`、endpoint、Source 和 EventType 属于诊断信息。部署环境如认为这些字段敏感，应通过上层 CR 权限控制限制访问。

### 7.2 Status 查询不得改变业务状态

Status 查询不得：

- 触发连接或重连。
- 发送 ping、subscribe 或 unsubscribe。
- 清理 Failed 订阅。
- 修改重试次数。
- 等待后台任务结束。

### 7.3 控制快照开销

- `status` 只返回汇总，不构造订阅明细列表。
- `remoteClients` 才构造客户端订阅明细。
- `serverSessions` 第一版只返回 session 级汇总。
- 不在 Status getter 内记录逐条日志。

如果客户端订阅规模后续达到数万级，应新增分页接口，不应继续无限扩大 `remoteClients` 响应。

## 8. 第一版暂不提供的接口

第一版不提供以下 `Cfet2Method` 或 `Cfet2Config`：

- 强制断开某个 server session。
- 强制重连某个 remote client。
- 清理 Failed 订阅。
- 动态修改 WebSocketCommunicationOptions。
- 重置累计指标。

这些操作会改变通信状态，需要单独设计鉴权、幂等、竞态和审计行为。

第一版也不提供累计流量指标，例如：

- 累计接收/发送消息数。
- 累计接收/发送字节数。
- 累计连接成功、失败、拒绝次数。
- 累计丢弃 fan-out 事件数。

这些指标需要侵入收发路径维护原子计数器，可作为第二阶段扩展。

## 9. 测试要求

### 9.1 Thing 接口测试

1. `WebSocketCMThing` 使用传入的模块实例，不创建新模块。
2. 所有 Get 和 Print 接口均能被 CFET2 探测为 Status。
3. Thing 构造参数为 null 时抛出 `ArgumentNullException`。
4. 模块未启动、已启动、已 Dispose 时均能安全查询状态。
5. Print 接口字段顺序稳定；明细为空时允许返回空字符串，输出不包含认证信息。

### 9.2 总体状态测试

1. 未启动时 `State = Created`。
2. 启动服务端后 `ServerStarted = true`。
3. 一个远程客户端连接本机后，`ServerActiveSessionCount = 1`。
4. 本机连接一个远端 endpoint 后，`RemoteClientCount = 1`。
5. 连接进入 `Connected` 后，`RemoteConnectedClientCount = 1`。
6. 远端连接或订阅失败后，失败计数正确；模块未释放且已启动时仍为 `State = Started`。
7. Dispose 后 `State = Disposed`，所有明细查询返回空集合。

### 9.3 服务端 session 测试

1. session 状态与底层 socket 状态一致，不能仅凭 `Open` 判断应用层握手成功。
2. hello/helloAck 完成后 `HandshakeCompletedAtUtc` 有值。
3. 收到消息后 `LastReceivedAtUtc` 更新。
4. 同一 session 创建多个订阅后 `RemoteSubscriptionCount` 正确。
5. session 断开后从 `serverSessions` 中移除。
6. `RemoteAddress` 正确且不包含认证信息。
7. `PrintServerSessions()` 能输出全部活动 session，空集合时返回空字符串。

### 9.4 远端客户端测试

1. 各 `RemoteConnectionState` 能正确映射到 DTO。
2. Pending、Active、Failed 订阅计数正确。
3. `remoteClients` 返回完整订阅明细，`status` 不返回明细。
4. 重连时 `RetryCount`、`LastError` 和 `LastReceivedAtUtc` 正确。
5. 多个本地 token 聚合为一个 master token 时，各计数语义正确。
6. 返回 DTO 后内部状态变化不会修改旧 DTO。
7. http://ip:port 能按照生效 WebSocketPortOffset（默认 1）查询对应 remote client。
8. `ws://ip:port` 能按照原 port 查询对应 remote client。
9. 查询不存在的 hostname 返回 null，格式错误或空白的 hostname 返回 null。
10. `PrintRemoteClients()` 和 `PrintRemoteClient(hostname)` 输出与结构化快照一致。

### 9.5 并发与可靠性测试

1. 并发连接、断开和查询 Status，不出现异常。
2. 并发订阅、取消订阅和查询明细，不出现集合修改异常。
3. 高频查询 Status 不阻塞连接、收发和 fan-out。
4. Status 查询不触发网络消息或重连。

## 10. 实施顺序

建议按以下顺序实现：

1. 新增 Diagnostics DTO，确定序列化字段。
2. 为 `ClientSubscriptionAggregator`、`RemoteEndpointConnection`、`RemoteEndpointActionProcessor` 增加内部诊断快照。
3. 为 `WebSocketSession`、`WebSocketSubscriptionManager`、`WebSocketEventServer` 增加服务端诊断快照。
4. 在 `WebSocketCommunicationModule` 增加三个公共查询接口。
5. 新增 `WebSocketCMThing` 并在应用层挂载。
6. 增加状态、并发和 Dispose 测试。

## 11. 第一版验收标准

第一版完成后，通过结构化 Get 接口和对应 Print 接口可以明确判断：

- WebSocketCM 当前是否正常运行。
- 当前有多少远程客户端连接到本机。
- 本机当前连接了多少远端 endpoint，各连接处于什么状态。
- 两侧当前分别有多少订阅，是否存在失败订阅。
- 具体失败发生在哪个 endpoint 或 master token。

所有查询均为只读快照，不影响现有连接、订阅、重连和事件传输行为。
