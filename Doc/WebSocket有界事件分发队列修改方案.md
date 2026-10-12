# WebSocket 有界事件分发队列修改方案

日期：2026-10-12。状态：已实现并通过全量回归，尚未提交或发布；第2节保留改造前行为作为对照，第10节描述未修改的 Core 行为。

## 1. 目标与范围

将服务端当前“并发分发达到上限就立即丢弃”改为：**有界等待队列＋固定分发并发数＋满队列时丢弃新任务并累计计数**。

本次实现范围为 WSCM 服务端事件分发、配置、诊断和测试。保留消息协议、订阅 ACK、源端时间戳、完整/精简载荷以及 path＋参数订阅规则。本次及本方案均不修改 Core；第10节仅说明 Core 当前行为与 WS 队列的保护边界，不安排 Core 改造。

队列用于缓冲短时突发；持续输入超过发送能力时仍会丢弃。该方案不提供可靠投递、事件重放或端到端背压。

## 2. 改造前行为（历史对照）

`WebSocketSubscriptionManager.SubscribeLocal` 注册的 Core 回调调用 `ObserveFanOutAsync`。后者增加活动分发计数，超过 `MaxConcurrentFanOutEvents` 就立即退出并增加 `overloadDropCount`，没有事件等待队列。

进入 `FanOutAsync` 后，取聚合组订阅者快照，通过 `Parallel.ForEachAsync` 并行发送。`MaxFanOutConcurrency` 限制单个分发任务内部的订阅者发送并发。

`WebSocketSession.sendLock` 保证同一会话一次只执行一个 Socket 发送。等待该锁也计入 `ServerSendTimeout`。控制消息也使用会话发送通道；分发队列不会改变这一点。

另有两个独立机制，不能混用其配置：

| 机制 | 内容 | 当前限制 |
|---|---|---|
| 客户端 actionQueue | Subscribe / Unsubscribe 控制动作 | 无界，ActionQueueWarningThreshold 仅告警 |
| 服务端订阅准备队列 | 建立本地订阅、可能执行初读 | MaxConcurrentSubscriptionPreparations / MaxPendingSubscriptionPreparations |
| 本方案事件分发队列 | 已发生事件向远端订阅者发送 | 固定消费者＋有界等待容量 |

## 3. 配置与计量单位

保留 `MaxConcurrentFanOutEvents`，用作固定消费者数量，默认沿用现有处理器数量规则；运行期间不动态改变。保留 `MaxFanOutConcurrency`，控制每个任务的订阅者发送并发。

新增 `MaxPendingFanOutEvents`，实现默认值为 256，要求大于 0；它仅表示等待任务数，不含正在执行任务。256 是初始默认配置，需按实际载荷大小、突发量和允许延迟压测调整，不能据此承诺内存上限或吞吐。

一个队列项代表“一个事件向一个聚合订阅组分发”。同一事件匹配多个组会产生多个任务；一组有多个远端订阅者仍只占一个任务。因此丢弃 1 个任务不等于丢失 1 个客户端通知，也不一定等于丢失 1 个原始 Publish。

例如并发数 4、等待容量 256，稳定阻塞情况下最多保留 4 个执行任务和 256 个等待任务。内部订阅者发送操作的并发上界约为 `4 × MaxFanOutConcurrency`；实际同一会话仍串行。

## 4. 入队与固定消费者

使用 `Channel.CreateBounded<FanOutWorkItem>`，配置 `FullMode = BoundedChannelFullMode.Wait`、多生产者、按消费者数量设置读取选项，禁用同步执行延续。

生产端**只使用 TryWrite，不调用 WriteAsync，也不为每个事件创建等待入队的 Task**。这里使用 Wait 模式是为了让 TryWrite 在满队列时明确返回 false，便于可靠统计；不依赖隐式 DropWrite / DropOldest 的丢弃行为。

工作项包括聚合组实例、事件引用、入队时订阅者实例快照。采用当前事件载荷契约，不额外深复制任意业务对象；直接 Publish 的可变载荷仍由调用者负责一致性。任务出队后及时释放引用。

`SubscribeLocal` 两个回调分支改为同步 `TryEnqueueFanOut`：

1. 检查管理器与聚合组仍有效。
2. 在同一短临界区内检查队列容量；空目标不入队，已满时不分配订阅者快照。
3. 有空位时捕获当前订阅者快照，TryWrite 入队并立即返回。
4. 容量不足时丢弃此次新任务，增加满队列丢弃计数，按限频规则写日志。入队、出队及订阅提交共用 syncRoot 协调，日志在锁外执行。

启动固定数量的异步消费者，每个消费者顺序读取并 await 一个任务完成。移除现有 `ObserveFanOutAsync` 中“活动数超限丢弃”的逻辑。每项异常须隔离处理，不能使消费者退出导致容量永久下降。

等待、活动计数的更新应与队列操作使用明确的短临界区或等价原子协议，防止消费者先取出导致负计数。入队、关闭与拒绝原因判定应协调，避免将关闭造成的 TryWrite 失败记成容量溢出；锁内不得执行序列化、网络发送或业务回调。

## 5. 丢弃、诊断与日志

第一版固定采用“满时丢新任务”，保留已经排队的任务。暂不增加可切换策略，不合并同路径事件；后者会改变状态事件和业务事件的语义。

对外诊断至少包括：

| 字段建议 | 含义 |
|---|---|
| PendingFanOutEventCount | 当前等待任务数 |
| ActiveFanOutEventCount | 当前正在执行任务数 |
| MaxPendingFanOutEvents | 配置的等待容量 |
| MaxConcurrentFanOutEvents | 配置的执行并发数 |
| FanOutQueueFullDropCount | 本管理器生命周期内因容量不足丢弃的任务总数，long |
| PeakPendingFanOutEventCount | 等待任务数峰值 |
| FanOutShutdownDiscardCount | 关闭时清理的等待任务数，long；不含活动任务 |

可增加队列峰值，方便调参。每次因满队列拒绝入队都增加累计丢弃计数，不受日志限频影响；计数不在恢复时清零。日志包含等待数、活动数、容量、并发上限、累计丢弃和距上次报告的新增丢弃数。

首次溢出进入过载告警状态，等待数降至容量一半或以下（整数判断为 `pending <= capacity / 2`）时结束该状态。此阈值仅用于告警恢复，不参与入队限制：只要腾出一个位置，就可以接受新任务，无须等待队列降至一半。

首次满队列告警立即记录，后续满队列告警统一限制为每秒最多一次；短暂恢复不重置上次告警时间，再次过载也遵守此限频。恢复时输出一次摘要，关闭时补记尚未报告的丢弃数量，保证最后一个限频周期内的丢弃仍可见；摘要与满队列告警分别处理，避免重复累计已经报告的丢弃数量。

关闭拒绝、订阅失效、序列化失败、网络失败不计入 QueueFullDropCount。若提供相关指标，应单独命名并区分按任务或按接收者计数。

## 6. 订阅生命周期与顺序

目标订阅者应在 WS 入队时取快照，避免积压项在出队时重新枚举，将旧任务发给后来加入的订阅者。这只能保证 WS 入队之后的边界；Core 中更早积压的事件仍受现有订阅匹配时机影响。

发送前检查聚合组仍活动，且字典中相同会话/Token 对应的订阅者实例仍为入队时实例，确保更新 revision、退订及同名组重建不会复用旧任务。

将有效性检查传递到 `WebSocketSession.SendRemoteEventAsync`，复用 `SendPayloadAsync` 已有的 `isValid` 机制，在获取 sendLock 后、Socket.SendAsync 前再次检查。已开始发送的消息无法撤回；不承诺退订与网络发送之间绝对原子性。

Channel 保持成功入队项的 FIFO 取出顺序，但多个消费者的发送完成顺序不保证；Core 当前 Task.Run 也不保证发布顺序。若未来需要逐路径或逐订阅严格有序，应单独设计分区串行队列，不能仅将 WS 消费者数量设为 1 就宣称端到端有序。

## 7. 关闭与恢复

服务器使用独立的幂等释放标志，不能仅依据 `IsStarted` 或 `app` 是否存在决定是否释放。未启动、启动失败及正常关闭均须释放订阅管理器及其消费者；重复释放安全。

关闭顺序：禁止新入队、完成 Writer、取消消费者与本管理器发起的发送等待、释放订阅、清理未处理项。固定消费者任务必须被保存并观察异常，不能遗留未观察的后台任务。

本次选用关闭时放弃待发送项，不承诺排空。取消令牌传递到每个任务内部发送，并与会话令牌、发送超时链接；管理器关闭取消与网络错误分别处理，正常关闭取消不应被当成网络故障触发误报。

取消后，新增分发消费者合计最多等待 5 秒，不能持有管理器锁等待。超时记录一次告警，通过任务完成延续观察异常并释放剩余资源；仍被消费者使用的取消源等资源不得提前释放。该时限仅约束新增分发消费者的等待，不承诺整个服务器关闭耗时不超过 5 秒。

关闭弃置和满队列溢出分开统计，关闭时补记尚未报告的满队列丢弃数量。重新创建管理器使用新队列和新计数。队列腾出空位即可接受新事件，无须重新订阅，也不依赖过载告警状态是否已经结束。

## 8. 修改文件与实施顺序

1. `Options/WebSocketCommunicationOptions.cs`：新增容量配置、说明单位和合法范围。
2. `Server/Subscriptions/WebSocketSubscriptionManager.cs`：替换入口；队列/消费者可拆到新的 FanOut partial 文件，避免与订阅准备队列混淆。新构造参数追加在末尾，保留现有位置参数含义。
3. `Server/WebSocketSession.cs`：事件发送传递有效性检查及取消令牌，保留原消息格式与超时行为。
4. `Server/WebSocketEventServer.cs`：传递新增配置，以独立的幂等释放标志协调关闭，覆盖未启动、启动失败及重复释放。
5. 各层 Diagnostics partial、`Diagnostics/WebSocketModuleStatus.cs`、模块 GetStatus/PrintStatus：接入队列和丢弃指标。
6. `TestCode`：替换“并发满即丢弃”测试，新增有界队列、生命周期及诊断测试。
7. 同步 WSCM 使用文档及修改方案10.9实施记录；完成后记录实际回归结果。

## 9. 验收

- 使用可控阻塞 Socket，确认活动任务不超过 N、等待任务不超过 Q；填满后新任务丢弃且计数准确。
- 释放阻塞后，原已入队项可继续发送，后续新项可入队，消费者数量不因异常减少。
- 队列满后只腾出一个空位，即使等待数仍高于容量一半，新任务也可入队；告警状态保持至等待数降至容量一半或以下。
- 多生产者并发下计数准确；关闭与入队竞争不把关闭误计为容量溢出。
- 满队列告警每秒最多一次，短暂恢复及再次过载不重置限频时间；每次丢弃均计数，恢复摘要和关闭摘要补齐尚未报告的数量，诊断跨层一致。
- 同事件匹配多组、同组多个接收者的计数单位正确。
- 排队期间退订、更新 revision、同名组重建、新订阅加入以及等待 sendLock 期间失效均符合第6节。
- 关闭可终止等待、释放队列引用，不无限等待网络；关闭弃置与满队列丢弃分开。
- 未启动释放、启动失败释放、正常关闭及重复释放均正确清理消费者。
- 消费者合计等待上限为 5 秒；超时仅告警一次，任务最终结束后观察异常并释放剩余资源，使用中的取消源不提前释放；正常关闭取消不误报网络故障。
- 完整/精简载荷、参数化精确/正则订阅及源端时间戳回归。
- 运行 WS 全量测试及 Core 回归；任何测试结果必须由实际执行填写。

### 9.1 实施与验收结果（2026-10-12）

- 新增 `WebSocketSubscriptionManager.FanOut.cs`：有界 Channel、固定消费者、同步入队、丢弃计数、告警恢复、关闭取消及延迟清理。
- `WebSocketSession` 传递分发取消令牌与订阅有效性检查，在取得 sendLock 后再次验证；完整/精简消息格式和时间戳保持原契约。
- 服务器配置和幂等释放已接通；GetStatus/PrintStatus 已接入本节全部诊断指标。使用示例见 [WSCM 使用说明第9.1节](WebSocketCM%20实现说明与测试边界%20V1.0.md)。
- 新增 `WebSocketFanOutQueueTests` 15 项用例，更新现有满并发丢弃测试和旧聚合组回调测试；Core 未作修改。

| 验收范围 | 证据 |
|---|---|
| 队列/活动上限、丢新、一个空位立即接收、半容量恢复 | Capacity_DropsNewTasks_AcceptsOneFreeSlot_AndRecoversAtHalf |
| 多生产者、逐次丢弃统计、关闭摘要、发送取消 | ConcurrentProducers_RespectCapacityAndCountEveryDrop |
| 新接收者、revision 更新、退订重建、等待发送锁失效 | QueuedTargets / RemovedAndRecreatedGroup / WaitingForSessionLock 用例 |
| 异常后恢复、完整/精简载荷及源端时间戳 | ConsumerSurvivesSerializationFailure_AndPreservesPayloadTimestamp（两等级） |
| 未启动/启动失败/重复关闭、5秒超时后最终退出 | ServerDisposal / ShutdownTimeout 用例 |
| 跨层诊断、按聚合组计数、非法容量、关闭竞争、日志异常隔离 | DiagnosticLayers / InvalidCapacity / EnqueueRacingShutdown / ThrowingLogger 用例 |
| 参数化精确/正则订阅、传输及既有功能 | WS 全量回归 280/280，通过；0 失败、0 跳过 |
| Core 兼容回归 | Core 全量回归 264/264，通过；0 失败、0 跳过 |

实际执行命令（仓库根目录）：

```powershell
dotnet test websocketcommunicationmodule/TestCode/TestCode.csproj --no-restore --logger "trx;LogFileName=bounded-fanout-ws-full.trx"
dotnet test newcfet2core/Code/CodeTest/CodeTest.csproj --no-restore --logger "trx;LogFileName=bounded-fanout-core-full.trx"
```

结果分别位于上述测试项目的 `TestResults` 目录。WS 构建出现既有 NU1900（NuGet 漏洞数据源无法访问）、NLog 过时 API 及测试分析器提示，不影响此次构建和测试通过。本次没有执行容量压测，默认 256 不代表已经验证适合所有生产负载。

## 10. Core 上游现状分析

以下为当前 `newcfet2core/Code/CFET2Core/Event/EventHub.cs` 的实际行为。

### 10.1 两条事件入口

```text
本地 Publish / 自动观察 / PublishResourceIfChanged
  → TryPublish
  → pendingLocalEvent（无界 ConcurrentQueue<EventArg>）
  → AutoResetEvent 唤醒本地循环
  → 路由候选与过滤匹配
  → 每个匹配订阅一个 Task.Run
  → 本地业务回调 或 WS 聚合组回调
  → 本方案的 WS 有界队列 → 固定消费者 → 会话 sendLock → Socket

远端 WS 通知
  → ReceiveRemoteEvent（校验、规范化 Host）
  → pendingRemoteEvent（另一个无界 ConcurrentQueue<EventArg>）
  → AutoResetEvent 唤醒远端循环
  → Host 与事件过滤匹配
  → 每个匹配订阅一个 Task.Run → 业务回调
```

`Init` 用两个 LongRunning Task 启动本地、远端循环。它们被信号唤醒后不断 TryDequeue；信号只是唤醒机制，不是队列容量，也不是每事件一个线程。

### 10.2 Task.Run 的作用与第二层积压

循环只负责取事件、匹配和安排回调，不等待回调执行完毕。每个匹配订阅调用一次 Task.Run，将回调工作交给线程池。

Task.Run 不等于立即创建一个独立线程。线程池决定何时执行；Core 没有对已提交回调设置业务并发数或等待容量。因此事件队列已经出队，不意味着事件处理已经完成。

例如 1,000 个事件每个匹配 10 个 Core 订阅，可以提交约 10,000 个回调任务。WS 聚合组在 Core 看作一个订阅，同组多个远端接收者不会直接变成多个 Core 订阅任务，但匹配多个聚合组仍会产生多个任务。

可能积压的位置分别是：

1. 发布速度高于 Core 取出与匹配速度：pendingLocalEvent 增长。
2. 接收速度高于远端取出与匹配速度：pendingRemoteEvent 增长。
3. 匹配和提交很快，但业务回调慢或线程池繁忙：线程池待执行回调积压，Core 队列长度可能很小。

Core 当前未为上述积压配置容量、固定回调并发或溢出丢弃计数。线程池自身的调度不等同于框架提供的有界策略。

### 10.3 当前执行与关闭契约

回调任务开始时检查 Hub 是否关闭、Token 是否还在订阅字典；随后执行 handler，并捕获同步异常记日志。检查与真正执行之间仍存在并发窗口，已经运行的业务回调不会被强行中止。

Core 回调类型为 Action<EventArg>。它不能追踪 async void 回调后续异步部分；同步 try/catch 也不能保证捕获其异步异常。改造前 WS 回调显式丢弃 ObserveFanOutAsync 返回的 Task，由 WS 方法内部处理异步错误。本方案实现后，WS 回调仅做同步入队，固定消费者负责观察分发异常。

Dispose 停止观察、清空订阅、取消并唤醒循环，最多同步等待循环 5 秒，循环退出后清空事件队列；它不会等待用户回调全部完成，也未集中追踪每个 Task.Run。

### 10.4 WS 有界后能保证什么

WS 入口回调只做短时 TryWrite，可以减少回调中分发启动成本和 WS 网络阶段积压，但不能限制此前 Core 的 ConcurrentQueue 与 Task.Run 提交量。大量本地慢回调也可能占用线程池，间接延迟 WS 入队。

`TryPublish` 成功只表示进入 Core 本地队列；`PublishResourceIfChanged` 返回 true 后比较基准可已经更新。若随后 WS 队列满而丢弃，Core 不会回滚基准，相同值的下一次辅助发布可能仍返回 false。自动观察同样只以 Core 入队成功为依据。当前架构不保证每个远端都看到最终状态；如需断线/丢弃后补齐，需独立设计重读或重放契约。

有界任务数也不等于严格有界字节数：事件载荷、订阅者快照大小不同，且 MaxMessageBytes 是序列化后的消息限制，不是 Core 入队时的内存预算。

