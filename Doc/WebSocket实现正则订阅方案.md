# WebSocket 实现正则订阅方案

日期：2026-10-09  
状态：源码已实现；2026-10-23 当前项目版本为 WebSocket 1.1.0，固定依赖 Core 3.2.0。下文测试及未发布包说明保留原阶段含义，不代表当前交付状态。第 1—12 节为设计与验收要求，实际配置、测试及已知限制见第 13 节。

本次确认：异步准备、100ms 慢 Get 报警、保留 ACK 超时策略、revision 失败幂等、有限准备容量及重叠规则问题独立处理均已确定。下述慢 Get 监测已作为本次 Core 配套改动实现。

2026-10-14 文档同步：Core 参数化精确订阅已支持初读和指定路径主动事件，WS 含参数基础来源及详细路径使用纯事件订阅，不在 WS 服务端重复初读；所有含参数 getter 均不自动观察，包括默认参数和 params。旧阶段测试数量保留，下文当前行为按最新规则更新。

关联文档：

- [Core 正则资源观察实现文档](../../newcfet2core/Code/CFET2Core/doc/Core%20正则资源观察/Core%20正则资源观察实现文档.md)
- [Core 正则资源观察接入 WS 文档](../../newcfet2core/Code/CFET2Core/doc/Core%20正则资源观察/Core%20正则资源观察接入WS文档.md)

## 1. 目标与已确认边界

使 WS 正则订阅既能接收原有主动发布事件，又能在来源端启动匹配资源的 Core 自动观察。客户端继续提交原始 Source / EventType 条件，无须查询资源列表或逐资源发送订阅请求。

已确认的约束：

1. 资源目录视为固定，只在建立 Core 组合订阅时扫描一次，不处理运行中新增、移除或重注册。
2. 资源注册完成后允许订阅，不新增等待 Thing.Start 的机制；实际可读性由初次读取判断。
3. 正则组合订阅初次读取失败不自动重试；部分资源失败、跳过或零匹配不撤销原始事件订阅。
4. 精确资源订阅保留现有契约，不统一改为批量订阅的宽松失败行为。
5. 不扩展 WS 观察状态协议，不增加观察 ACK 字段、查询或状态推送。
6. WS 按聚合订阅持有 Core 组合对象；轮询、变化判断和资源共享由 Core 负责。
7. 当前带参数 Get 的自动观察能力仍存在限制，本次 WS 接入不补齐该能力。
8. 本次包含精确及组合订阅的锁外异步准备，接收循环继续处理控制消息；不修改精确订阅原有读取结果契约。
9. 内部 Get 应快速返回，默认达到 100ms 记录 Error，阈值可配置；监测执行期间的慢读，不强制中止 getter。
10. 保留客户端 ACK 超时、同 revision 重发及耗尽后断线重连策略；不增加准备中消息。
11. 新版本失败保留最后成功提交状态，同 revision 重复请求复用成功或失败结果；更高版本覆盖旧的在途请求。
12. 准备执行及等待需求有界；容量满时拒绝新需求并保持连接，不新增容量恢复后的自动重试。
13. 重叠正则重复路由先验证；发现既有问题后独立记录和设计，本次不承诺新增跨聚合去重。

## 2. 当前实现与接入位置

`WebSocketSubscriptionManager.SubscribeLocal` 改造前分支为：

```text
精确命中已注册本地资源 → SubscribeResource
其他来源条件           → Subscribe（仅过滤事件）
```

本次已修改为：

```text
精确命中已注册本地资源 → SubscribeResource（保留现有行为）
其他来源条件           → SubscribeResources（过滤事件，并尝试建立观察）
```

“其他来源条件”包括正则表达式及未精确命中注册资源的事件来源。保留现有精确路径识别方式：查找资源后，还必须确认资源实际 Path 与请求 Source 相等，不能将查找时回退得到的父资源当作精确命中。

来源端构造的 LocalFilter 保持 Host 为空、PerformanceLevel 为本地值；远端传输精简程度仍由各 RemoteSubscriber 的 PerformanceLevel 决定。

概念代码如下，实际调用须放入第 6 节的准备流程：

```csharp
var subscription = hub.SubscribeResources(
    aggregated.LocalFilter,
    eventArg => /* 向该聚合实例的订阅者分发 */);

aggregated.LocalToken = subscription.Token;
aggregated.LocalSubscription = subscription;
```

`AggregatedServerSubscription.LocalSubscription` 已为 `IDisposable`，可直接保存 `ResourceGroupSubscription`；WS 不额外复制或自行维护它内部的观察句柄列表。

### 2.1 正则展开与 Get 的执行方

不存在 `Get("/device/.*")` 这样的正则读取。客户端发送原始条件；来源端 Core 匹配本地已注册资源，再对符合观察资格的具体资源调用无参数 `resource.Get()`。初值供变化比较使用，不作为初始 changed 返回客户端；后续轮询也由来源端 Core 执行。

组合订阅复用已有资源观察时只取得共享句柄，不重新初读。精确 `SubscribeResource` 有自身初值读取契约，新建精确订阅不能据此省略读取。远端 Host 的 Core 组合订阅仅路由事件条件，不在订阅客户端展开目录或执行这些本地 Get。

## 3. 正则订阅接收哪些事件

`SubscribeResources` 同时保留原始事件过滤器，并为当前匹配且具备观察资格的资源获取句柄。观察资源集合不能替代原始事件过滤条件。

例如 Source 为 `/device/.*`，EventType 为 `.*`：

| 事件来源 | 能否接收 | 是否依赖本次启动轮询 |
|---|---|---|
| 业务主动发布 `/device/Temperature` 的 `alarm` | 能 | 否 |
| 原生事件资源发布 `/device/Temperature` 的 `changed` | 能 | 否，原生事件资源不额外轮询 |
| Core 轮询产生 `/device/Pressure` 的 `changed` | 能 | 可由本次订阅或其他有效持有者维持观察 |
| 未注册路径 `/device/custom/path` 主动发布事件 | 能 | 否 |
| `/other/Temperature` 发布事件 | 不能 | 来源不匹配 |

Source 与 EventType 必须同时匹配。Core 使用完整匹配语义，并按配对条件进行 OR 匹配：多组条件不是来源列表与事件类型列表的任意交叉组合。

轮询产生的事件类型为 `changed`。只有规则能匹配 `(resource.Path, "changed")` 时，资源才会进入本次观察候选集合。因此仅订阅 `alarm` 可接收主动 alarm，但不会为此启动 changed 轮询。

以上范围是当前 WS 服务端所在 Hub 接收和分发的匹配事件，不会自动发现或订阅其他远端 Hub。建立期间不提供历史事件补发，也不新增“ACK 前绝不收到事件”的顺序保证。

准备期间尚未提交的新请求不接收、不缓存补发事件；更新时旧的已提交订阅继续生效。新请求提交后可以接收事件，事件与 ACK 的网络先后顺序仍不作额外保证。

## 4. 聚合、句柄与释放

```text
远端订阅 (SessionId, RemoteTokenId)
    → 一个或多个聚合项 (Source, EventType)
    → 每个聚合项一个 Core 本地订阅对象
        → 一个事件 Token
        → 零个或多个单资源 ObservationHandle
```

事件 Token 标识事件订阅；ObservationHandle 标识资源观察持有关系。多个资源句柄不会变成多个 WS 远端 Token。

例如当前资源为温度、压力，A、B 订阅 `/device/.* + changed`，C 订阅 `/device/Temp.* + changed`：

```text
聚合 R：A、B → Token R + 温度句柄 W1 + 压力句柄 W2
聚合 S：C   → Token S + 温度句柄 W3

Core 温度：W1、W3 → 共享一个轮询任务
Core 压力：W2     → 一个轮询任务
```

| 操作 | WS 处理 | Core 结果 |
|---|---|---|
| A 退订 | R 仍有 B，保留组合对象 | 观察继续 |
| B 退订 | R 无需求，释放 Token R、W1、W2 | 压力停止，温度由 W3 继续维持 |
| C 退订 | 释放 Token S、W3 | 若无本地或其他模块持有者，温度停止 |

WS 仍按连接及 RemoteTokenId 发送消息；多个连接共享资源读取，不意味着可以省略各自的消息发送。后续 MQTT 按资源续租，是另外一种生命周期策略，与 WS 共同使用 Core 的资源共享表。

## 5. 聚合键必须保留正则语义

改造前客户端、服务端存在对 Source / EventType 使用忽略大小写比较、过滤器签名整体转大写的逻辑。正则文本不能直接这样归一化，例如 `\d` 与 `\D` 意义相反，即使 Core 匹配使用 IgnoreCase，也不能将二者合并。

本次接入拟同步调整客户端聚合、服务端聚合、条件去重和 revision 签名：

- 对确定不含正则语法的普通字面条件，保留原有忽略大小写聚合行为。
- 对含正则语法的条件，按原始文本进行 Ordinal 比较，不转大写。
- Source、EventType 分别判定；相等比较、哈希、去重与签名必须采用同一规则。
- 判定采取保守方式：含反斜杠或正则元字符的文本按正则处理，不尝试推导两个正则是否语义等价。
- 签名采用无歧义的结构化或长度编码方式，不能因简单分隔符拼接导致不同条件碰撞。

此规则只决定是否共享聚合项，不修改传给 Core 的表达式，也不改变 Core 的实际匹配规则。宁可让语义相同但写法不同的正则分别建组，也不能错误合并语义不同的表达式；底层同资源轮询仍可由 Core 共享。

## 6. 建立过程与并发设计

### 6.1 避免在管理锁中读取设备

改造前 Session 在 `lifecycleLock` 内调用同步 Subscribe，管理器又在 `syncRoot` 内调用 Core。正则展开可能连续读取多个 getter，直接替换调用会扩大阻塞时间，影响其他订阅、退订和关闭。

采用“锁内预留、锁外准备、锁内提交”的流程：

1. 校验协议、条件、PerformanceLevel、revision，生成请求标识。
2. 锁内检查会话与管理器状态，为缺失聚合项登记准备状态；已有项直接复用。
3. 相同聚合键的并发请求共享一个准备任务，并记录各请求的待提交持有关系。
4. 锁外执行 Core 建立调用。Core 同步 getter 工作必须移出接收循环执行路径，异步方法本身不保证这一点。
5. 准备结束后，锁内重新校验会话存活、请求标识、revision 及聚合实例归属。
6. 仍有效时一次提交该请求的全部聚合关系与 remoteIndex，然后移除旧关系。
7. 失效或失败时释放该请求的预留关系；聚合项既无已提交订阅者也无有效准备需求时，移除并释放组合对象。
8. 释放对象和网络发送均放在管理锁外执行，释放异常记录诊断。

准备状态、有效请求和已提交索引需区分管理。不能仅依据 Subscribers 为空就释放仍被其他准备请求需要的聚合项。

### 6.2 Session 接收循环

改造前接收循环等待 SubscribeAsync 完成后才读下一条消息。接入后仍保持单一接收循环，但订阅准备以可跟踪任务运行，使 ping/pong、退订和关闭可继续处理。

必须满足：

- 接收时先登记请求顺序及身份，再异步准备，避免线程调度倒置订阅与退订的先后关系。
- 同一远端 Token 的在途更新纳入 revision 管理；重复请求等待或复用同一结果。
- 退订、更新覆盖和会话关闭会使对应在途请求失效，旧任务完成后不能复活订阅。
- 跟踪任务并观察其异常，禁止无管理的后台任务；准备任务及排队需求需要有界控制，避免慢 getter 导致无限堆积。
- ACK 保留 TokenId、Revision、Accepted、Reason，仍通过现有发送串行化机制发送；失效请求不得发送迟到的成功 ACK。

精确及组合订阅统一经过此流程。即使每个 Get 少于 100ms，多个初读累计也可能较长，不能重新放回全局管理锁或接收循环内。具体内部类型由实现决定，不增加协议字段。

### 6.3 Core 同步读取的取消边界

当前 Core `SubscribeResources` 在返回前同步完成扫描与初次读取，没有外部取消入口。WS 能立即使准备请求失效并继续关闭流程，但不能强制中断已执行的 getter。

如果一个组合已取得部分资源句柄、随后卡在另一个 getter，WS 暂时拿不到完整返回对象。这些句柄需等 Core 调用返回后才能由迟到清理释放；若 getter 永不返回，无法承诺立即释放全部准备期资源。

本阶段保留并明确该限制，不把任务取消写成设备读取已终止。如果后续要求立即释放准备期已取得句柄，应另行扩展 Core 的取消或提前登记契约。

已取消但仍执行的 Core 调用继续占用执行名额，直到实际返回并完成清理；不能仅因外层等待被取消就释放名额并启动替代读取。慢读 Error 只提供诊断，不改变这个生命周期限制。

### 6.4 事件回调绑定具体聚合实例

改造前回调按聚合键查询当前字典项。旧聚合项释放后，同键新项可能已经建立，旧回调不应向新项的订阅者分发。

回调应绑定创建时的聚合实例，并检查该实例是否仍有效；异步分发不能仅凭键重新选择实例。退订不承诺撤回已经进入网络发送的消息，但失效实例不得产生新的有效分发关系。

### 6.5 准备容量与满载行为

服务端设置有限的 Core 准备执行并发及有限等待容量，均可配置；具体名称和默认数值由实现确定并记录，测试用小容量验证边界。等待需求及尚未执行的工作都必须有界，不能只限制运行任务却无限累积等待对象。

- 相同聚合项共享已有准备任务，不重复占用执行名额；新的独立等待需求仍受等待容量约束。
- 同请求同 revision 重发合并处理，不为每次重发追加任务、等待对象或句柄。
- 容量不足时使用现有 `Accepted=false` 和明确 Reason 拒绝，连接及已提交订阅保持；本次已预留但不再使用的关系回滚。
- 容量拒绝属于该 revision 的失败结果，重复请求复用该结果。容量恢复后，由调用方以更高 revision 或新的订阅生命周期重新发起，不自动重试。
- 排队请求失效后及时移除；执行中的请求按 6.3 等实际调用返回后释放名额。关闭时不再执行未启动工作。
- 复用已就绪聚合项不需要新的 Core 执行名额；不能因新建任务满载而无条件阻断该复用路径。
- 采用公平的有界调度，不允许多条件请求占用全部工作名额再等待同一执行池中的子任务，避免调度死锁。

### 6.6 Core 慢 Get 监测

这是本次已实现的 Core 配套能力，WS 不另建重复计时。覆盖组合观察初读、后续轮询，以及本地精确 SubscribeResource 的初读；不将远端网络 Get 或所有普通业务 Get 一并纳入此次改造。

| 项目 | 已确认行为 |
|---|---|
| 阈值 | 默认 100ms，可配置为正的有限时长；配置读写及非法值处理遵循 Core 配置约定并测试 |
| 计时 | 单次实际 `resource.Get()` 调用，使用单调计时；排队、等锁、正则匹配和消息发送不计入 getter 耗时 |
| 未返回时 | 独立于该 getter 的监测能发现达到阈值仍在执行的读取并记录 Error；不保证操作系统调度下恰好第 100ms 输出 |
| 返回时 | 补充最终耗时；即使监测尚未调度，返回后的计时也能发现超阈值读取，避免漏报 |
| 结果处理 | 慢但有效的结果继续使用；无效 Sample、异常仍按原来的读取失败语义处理 |
| 生命周期 | 不强制中断、不新增重试、不因慢读停止观察，也不额外并发读取同一资源 |
| 诊断 | 包含资源路径、初读/轮询阶段、读取身份、阈值及已耗时/最终耗时；同次告警与完成记录可关联，按资源限频，保留被抑制次数或汇总 |
| 监测清理 | 正常返回和异常结束均注销监测；取消或关闭不得由迟到监测触发业务提交，监测器不阻塞关闭 |

采用可管理的共享监测设施，避免每个 Get 启动一个阻塞等待线程。正在执行和已经完成的记录通过身份区分，避免前一次迟到计时影响下一次读取。监测设施自身不能持有设备读取锁或 WS 管理锁执行日志回调。

当前默认轮询延迟为 500ms，实际周期还包括读取耗时。100ms 是性能违规报警线，不是硬实时保证，也不改变轮询节奏。它与 Core 正则单次计算的 100ms 限制是独立配置、独立语义，不得共用一个超时开关。

## 7. revision、回滚与重试

revision 是同一 `(SessionId, RemoteTokenId)` 订阅的请求版本，不是资源版本或事件序号。重发相同请求保持版本不变，修改条件或性能级别使用更高版本。

管理器区分“最后成功提交的订阅”和“当前生命周期已处理的最高请求版本及结果”。最新版本失败也不降低版本水位；失败结果保存到更高版本替代或退订/会话关闭，不需要无限保存全部历史版本。

| 场景 | 处理原则 |
|---|---|
| 低于已处理最高请求版本的请求 | staleRevision 拒绝，不因最新请求失败而接受旧版本 |
| 同 revision、相同条件和性能级别 | 复用在途准备或成功/失败结果，不重复获取句柄 |
| 同 revision、不同内容 | 沿用 revisionConflict 拒绝语义 |
| 较高 revision，仅性能级别改变 | 更新订阅者发送设置，保留 Core 组合对象 |
| 较高 revision，条件改变 | 先准备新关系，成功后切换，再释放旧关系 |
| 新条件建立失败 | 回滚本次新增需求，保留此前已提交的有效订阅 |
| 退订或关闭发生在准备期间 | 使请求失效；迟到结果只能清理，不能提交 |
| 更新的请求版本到达 | 使旧的在途请求失效；新请求失败也不恢复旧在途请求 |
| 同 Token 退订后重新订阅 | 新内部生命周期身份；旧任务和旧结果不能影响新订阅 |

例如 revision 1 已生效，2 尚在准备，3 到达后失败：保留 1，2 不再提交；重复发送相同 revision 3 返回失败结果，主动重新尝试应使用更高版本或新的订阅生命周期。

较高版本在途请求与旧结果之间必须通过请求身份区分，不能让旧准备结果覆盖新状态。多条件原子提交只覆盖单条 WS Subscribe 请求：其中某条件整体建立失败时不得只提交其他条件。现有客户端会把本地多条件拆成多个远端聚合请求，本次不增加跨消息事务，因此不承诺整个本地多条件订阅原子成功。

逐资源初次读取失败不是整体建立失败，不触发 WS 自动重试。复用同一聚合项、加入新客户端或仅提高 revision 都不会重新读取失败资源。只有实际新建 Core 组合对象才重新尝试；取消后重订阅若仍复用其他订阅者持有的组，也不保证重试。

## 8. ACK 与来源端诊断

| 情况 | 对外结果 |
|---|---|
| 正则合法，部分资源 Failed / Skipped | 接受事件订阅 |
| 零观察资源、原生事件或只匹配 alarm | 接受事件订阅 |
| 无效正则、初始匹配超时或整体建立异常 | 通过现有拒绝 ACK 和 Reason 反馈，回滚本次新增状态 |
| 精确资源初次读取失败 | 保留现有精确订阅失败语义 |

Accepted 表示接受事件订阅，不表示全部匹配资源可读。逐资源 Started、Shared、Skipped、Failed 结果保留在来源端组合对象与诊断中；它们只是建立时结果，不是持续健康状态。避免 WS 与 Core 对同一失败重复刷日志。

Core 正则超时为单次正则计算 100ms，不是订阅总时限，更不是 getter 超时。初始扫描超时拒绝建立；事件分发匹配超时由 Core 限频记录并跳过本次匹配，不自动退订，也不阻断其他订阅。

Core 当前有已记录缺陷：初次读取返回无效 Sample 时，观察结果可能只保留通用原因，丢失 Sample 中的具体错误信息。该问题尚待修复，不能承诺 WS 诊断已提供完整设备错误。

### 8.1 ACK 等待、协议重发与资源读取

保留现有客户端 `SubscribeAckTimeout`（当前默认 10 秒）、`MaxSubscribeRetryCount`（当前默认 3）及重试耗尽后中止连接并重连的行为。不新增准备中消息，也不把 Get 慢读阈值当作 ACK 超时。

同 Token、同 revision 的协议重发复用在途准备或已完成结果，不再发起一组 getter。正在准备时无需提前发送成功 ACK；完成后仅对仍有效请求反馈结果，已失效请求不得发送迟到成功结果。

即使心跳正常，准备总耗时超过客户端等待预算仍可能导致整条连接重连，影响同连接其他订阅。部署可以调整现有等待配置，本阶段不改变这一策略。协议重发不等于资源初读重试；断线释放旧组后实际新建组合对象可以再次初读，复用仍被其他有效订阅者持有的组则不重新初读。

更新拒绝时，服务端保留旧的已提交订阅；现有客户端对当前请求的诊断可以显示 Failed，不代表旧的服务端关系已被撤销。本次不扩展状态协议，测试须分别检查服务端实际关系与客户端请求状态。

## 9. 带参数 Get 与正则写法注意事项

假设注册资源仅有 `/device/Temperature`，对应 getter 为 `Temperature(int channel)`：

| 条件 | 当前行为 |
|---|---|
| Core 组合订阅 `/device/Temperature + changed` | 匹配基础资源，但必填参数无法提供，观察结果为 Skipped；事件过滤仍有效 |
| WS 精确来源 `/device/Temperature` | 含参数基础来源走纯事件订阅，不在 WS 服务端初读或观察；Core SubscribeResource 调用者仍须先通过资源模块取得有效初值 |
| `/device/Temperature/.* + changed` | 不匹配基础注册路径，观察集合为空；仍可接收 `/device/Temperature/1` 等主动发布的匹配事件 |
| `/device/Temperature/* + changed` | `*` 重复前面的 `/`，不是通配符；可匹配基础路径及尾随斜杠，不能表示任意 channel |

当前不会枚举参数取值，不会自动观察所有 channel。单资源 SubscribeResource 即使通过带后缀路径找到基础资源，也不能据此认为参数已传给观察读取；普通 Get 支持参数不等于自动观察支持参数。

全部读取参数有默认值或为 params 时，自动观察入口仍跳过该资源，不执行 getter。Core 精确 SubscribeResource 可按地址读取默认值或指定参数的初值，之后接收主动事件；这不建立轮询。完整支持需要另行设计“资源 + 参数”的共享键、初值、事件来源路径和参数枚举规则。

## 10. 文件修改范围

以下为本次实现涉及的文件与职责；具体结果见第 13 节。

| 文件或位置 | 计划修改 |
|---|---|
| `Server/Subscriptions/WebSocketSubscriptionManager.cs` | 接入 SubscribeResources；准备/提交与回滚；在途请求；实例绑定回调；锁外清理与诊断 |
| `Server/Subscriptions/AggregatedServerSubscription.cs` | 保留 IDisposable 持有；按需补充准备任务、实例状态及待提交需求管理 |
| `Server/Subscriptions/ServerSubscriptionKey.cs` | 保留正则文本语义的比较与哈希 |
| `Server/Subscriptions/RemoteSubscriber.cs` 及内部辅助类型 | 按需承载已提交状态与在途请求身份，避免将两者混为一体 |
| `Server/WebSocketSession.cs` | 跟踪异步准备；接收循环继续处理控制消息；关闭与迟到结果清理 |
| `Connections/ClientSubscriptionGroupKey.cs` | 与服务端一致的正则条件比较 |
| `Client/ClientSubscriptionAggregator.cs` | 修正条件去重，避免客户端提前错误合并正则 |
| `Options/WebSocketCommunicationOptions.cs` 及配置入口 | 增加准备执行和等待容量配置，保持现有 ACK 超时配置语义 |
| Core `Event/ResourceObservationManager.cs`、`Hub.cs` 及监测辅助类型 | 统一覆盖组合初读、轮询及本地精确初读的慢 Get 监测，不重复读取 |
| Core `Config/GlobalConfig.cs` 及配置入口 | 增加独立慢读阈值，默认 100ms，支持配置及校验 |
| Core `CodeTest/` | 增加慢读监测与现有读取语义兼容测试 |
| `TestCode/` | 新增正则观察与竞争测试，调整非精确来源订阅类型断言，保留既有行为断言 |

保留现有协议序列化、完整/精简事件模式、源端时间戳和分发限流语义。客户端键比较调整不需要新增消息格式。本阶段不改写既有跨聚合项事件路由或重叠订阅的整体去重策略。

## 11. 测试与验收

以下矩阵规定验收范围，实际执行记录见第 13 节。各编号应映射到具体测试方法；一项可由参数化测试或多个测试共同覆盖。不能仅凭收到一次 changed、测试数量或代码覆盖率判定完成。

### 11.1 测试分层与共同断言

| 层级 | 主要验证内容 |
|---|---|
| 条件与状态单元测试 | 聚合键、去重、签名、revision、请求失效和状态转换 |
| WS 管理器与真实 Core 集成测试 | 观察资格、句柄共享、回滚、清理和可控 getter 竞争 |
| 真实 WS 端到端测试 | 客户端聚合、服务端观察、网络传输、客户端 Hub 回调、重连和退出 |
| 既有回归 | 精确订阅、协议、时间戳、性能级别、心跳、发送异常及 Core 底层能力 |

按场景同时检查对外结果和内部生命周期：ACK、事件来源/类型/值/时间戳、RemoteTokenId、聚合项数、事件订阅数、句柄归属、观察任务数、getter 调用次数、在途需求数。不能仅断言轮询数量，因为轮询共享正常时仍可能泄漏句柄。

每个用例结束后，状态应恢复到测试前基线；如果故意保留其他本地或远端订阅者，应验证其持有关系和观察继续存在，而不是要求所有计数无条件归零。

### 11.2 正则匹配与事件范围

| 编号 | 场景 | 验收结果 |
|---|---|---|
| F01 | 无其他观察者，`/device/.* + changed` 命中多个资源 | 独立启动所有合格资源观察，各资源变化后收到正确事件 |
| F02 | `/device/Temp.*` 命中部分资源 | 不匹配资源不被读取、不被观察 |
| F03 | 完整匹配及路径前后缀边界 | 不因子串相同而误匹配更长路径 |
| F04 | 来源匹配但类型不匹配，以及反向情况 | 均不向该订阅发送事件 |
| F05 | 多组 Source / EventType | 配对 OR 生效，不产生交叉组合；包含应接收和不应接收样例 |
| F06 | EventType 为 `.*` | 同时接收轮询 changed 和主动 alarm |
| F07 | 仅订阅 alarm | 接收主动 alarm，不启动 changed 观察 |
| F08 | 原生事件资源 | 不额外轮询，主动发布仍可接收 |
| F09 | 未注册路径主动发布 | 原始 regex 匹配即可接收，观察目录不限制事件过滤 |
| F10 | 不支持观察或初读失败的资源主动发布 | 事件订阅仍有效 |
| F11 | 零资源匹配 | ACK 接受、观察集合为空，之后匹配的主动事件可接收 |
| F12 | 范围外事件及来源端 Host 转换 | 不发送不匹配事件；本地过滤器 Host 为空，不触发再次远端订阅 |

### 11.3 观察资格与读取行为

| 编号 | 场景 | 验收结果 |
|---|---|---|
| O01 | 支持的 Status、Config 资源 | 正常观察，来源端结果为 Started 或 Shared |
| O02 | 原生事件、Method、不支持的复杂类型与可观察资源混合 | 按 Core 规则跳过相应资源，其他观察不受影响 |
| O03 | 初读成功后值不变，再改变 | 初值不作为 changed 发送；不变不重复发送，变化后发送正确值 |
| O04 | 初读抛异常或返回无效 Sample | 对应资源 Failed，其他资源继续，组合 ACK 接受 |
| O05 | 初读失败后设备恢复 | 不后台重试，不自行补建观察 |
| O06 | 已成功观察，运行期读取失败后恢复 | 保持订阅，沿用 Core 恢复行为，无需重新订阅 |
| O07 | 注册完成但 Start 尚未完成 | 不等待 Start，按实际读取结果处理 |
| O08 | 建立后注册新的匹配资源 | 不自动补扫，不启动该新资源观察；仅验证固定快照边界，不扩展动态目录功能 |
| O09 | 初读全部失败或全部跳过 | 事件订阅仍可接受，无无效观察句柄残留 |

Core 已覆盖的类型、基线和读取恢复细节可复用底层回归；WS 层仍需验证不会将部分失败误转为整体拒绝、重复初读或自动重试。

### 11.4 带参数资源边界

以下假设仅注册 `/device/Temperature`，getter 为 `Temperature(int channel)`。

| 编号 | 请求或场景 | 验收结果 |
|---|---|---|
| P01 | WS 精确 `/device/Temperature`（含参数 getter） | 接受纯事件订阅，不执行来源端初读或自动观察 |
| P02 | regex `/device/Temp.*` | 匹配基础资源，观察 Skipped；匹配的主动事件仍可接收 |
| P03 | `/device/Temperature/.*` | 观察集合为空，不枚举 channel；可接收 `/device/Temperature/1` 主动事件 |
| P04 | `/device/Temperature/*` | 按正则量词解释，不当作任意子路径通配符 |
| P05 | `/device/Temperature/1` | 不因查找回退而错误启动基础资源观察 |
| P06 | getter 参数全部有默认值 | 跳过自动观察，不执行 getter；主动事件过滤仍有效 |

这些是当前限制的边界测试，不能将 ACK 接受写成带参数观察成功。

### 11.5 聚合、共享与释放

| 编号 | 场景 | 验收结果 |
|---|---|---|
| L01 | 两个 Session 相同 regex | 一个聚合项和一组句柄，两个客户端分别收到对应 Token 的事件 |
| L02 | 同 Session 不同 Token 相同条件 | 共享聚合项，各订阅关系独立 |
| L03 | 不同 Session 使用相同 TokenId | 退订、更新和发送不串线 |
| L04 | 不同 regex 命中相同资源 | 各组独立句柄，Core 单资源只有一个轮询任务 |
| L05 | regex 与 WS 精确订阅重叠 | 共享 Core 观察，分别释放互不干扰 |
| L06 | regex 与本地订阅重叠 | WS 退出后本地观察继续，最后一个 Core 持有者退出才停止 |
| L07 | 复用已就绪聚合项，或组合订阅取得已有观察句柄 | 不重新初读、不重置基线；另设对照确认新建精确 SubscribeResource 保留自身初值读取契约 |
| L08 | 同组一个订阅者退出，再退出最后一个 | 先保留观察，最后释放该组 Token 和所有句柄 |
| L09 | 多条件订阅取消 | 清理所有关联，其他订阅者共享的项保留 |
| L10 | 重复退订、重复 Dispose | 幂等，无重复释放副作用 |
| L11 | 完全退订后同条件重订阅 | 建立新的有效对象，不复用失效实例 |

### 11.6 聚合键、去重与签名

以下场景须同时覆盖客户端与服务端，避免客户端先错误合并，导致服务端测试无法发现问题。

| 编号 | 场景 | 验收结果 |
|---|---|---|
| K01 | 普通字面条件仅大小写不同 | 保留原有忽略大小写聚合行为 |
| K02 | Source 中 `\d` / `\D`、`\w` / `\W` | 不错误合并，实际事件匹配正确 |
| K03 | EventType 中具有不同含义的正则 | 与 Source 一样保留原始正则语义 |
| K04 | 相同正则重复、条件顺序改变 | 去重且签名一致，不重复建立 |
| K05 | 语义相同但文本不同的正则 | 允许分别建组；底层观察仍可共享 |
| K06 | 条件包含签名分隔字符、不同条件集合 | 签名无碰撞，不错误判为同 revision 内容 |
| K07 | 包含正则元字符的已注册路径 | 精确分支识别与过滤行为符合既有契约 |

### 11.7 revision、更新与回滚

适用场景须分别覆盖已完成订阅和在途准备，不能只测试同步完成后的状态。

| 编号 | 场景 | 验收结果 |
|---|---|---|
| R01 | 同 revision、相同内容重复提交 | 复用已有或在途结果，不重复获取句柄 |
| R02 | 同 revision、不同内容 | 冲突拒绝，不破坏有效订阅 |
| R03 | 旧 revision | 不覆盖较新状态 |
| R04 | 提高 revision，仅改性能级别 | 保留 Core Token 和句柄，使用新发送模式 |
| R05 | 提高 revision，修改条件成功 | 新关系准备成功后切换，释放不再需要的旧关系 |
| R06 | 新条件整体建立失败 | 旧订阅继续接收事件 |
| R07 | 多条件前项准备成功，后项整体失败 | 新增关系全部回滚，不部分提交 |
| R08 | 回滚项同时被其他请求使用 | 不释放其他请求的有效需求 |
| R09 | 更新中新资源部分初读失败 | 组合仍可成功，不误判整体失败 |
| R10 | 新客户端加入、重复请求、更新 revision 但复用聚合项 | 不重新尝试该组初读失败的资源 |
| R11 | 完全释放后实际新建组 | 可以重新尝试初读，区别于复用现有组 |
| R12 | 1 已提交，2 在途，3 覆盖 2 后失败 | 1 继续工作；2 迟到成功也不提交，资源正确清理 |
| R13 | 最新版本失败后同内容重发、同版本改内容、发送更旧版本 | 分别返回原失败、revisionConflict、staleRevision；无新初读 |
| R14 | 最新失败后主动提高版本重新尝试 | 可创建新的准备请求，不被旧失败结果永久阻断 |
| R15 | 失败结果缓存后退订、断连或模块关闭 | 清理对应缓存；新生命周期不继承旧失败，旧回调无效 |
| R16 | 一个本地多条件被客户端拆成多个 WS 请求，其中一个失败 | 各请求独立提交；不宣称跨消息原子性，已成功请求可正常释放 |
| R17 | 更新被拒绝但旧服务端订阅保留 | 旧关系仍有效，客户端最新请求按现有逻辑标记失败；新版本 ACK 不被旧版本 ACK 覆盖 |

R12—R17 按第 7 节已确认规则验收，分别覆盖正常失败与容量拒绝；不能依据线程完成顺序改变预期。

### 11.8 异步准备与生命周期竞争

| 编号 | 可控竞争场景 | 验收结果 |
|---|---|---|
| C01 | A 会话 getter 阻塞，B 会话订阅或退订 | B 可以推进，不被全局管理锁阻塞 |
| C02 | 同连接订阅准备期间收到 Ping | 能回复 Pong，接收循环未被 getter 占用 |
| C03 | 准备期间退订 | 请求失效，getter 返回后不能复活订阅或发送迟到成功 ACK |
| C04 | 准备期间会话断开 | 已提交关系清理，迟到准备结果释放 |
| C05 | 准备期间模块 Dispose | 关闭不无限等待 getter；迟到结果不提交 |
| C06 | 两个同条件请求并发准备 | 只创建一个聚合准备任务 |
| C07 | 同组一个等待者取消 | 其他有效等待者仍可完成 |
| C08 | 所有等待者取消 | 准备返回后释放全部无主对象及预留状态 |
| C09 | 旧 revision 慢、新 revision 快 | 旧结果不能覆盖新状态 |
| C10 | 退订后立即以同 Token 重订阅 | 旧任务不能删除、覆盖或清理新请求 |
| C11 | 同键旧聚合移除后新建聚合 | 旧回调不能向新实例的订阅者分发 |
| C12 | 准备完成与取消同时发生 | 提交或释放只有一个有效结果，无重复持有或泄漏 |
| C13 | 已取得部分句柄，后续 getter 阻塞 | 验证暂时保留的已知限制；解除阻塞后迟到对象和句柄全部清理 |
| C14 | 准备任务或队列达到上限 | 按明确的有界策略处理，不无限堆积，控制消息仍能处理 |
| C15 | 准备期间到达事件及开始分发后发生退订 | 不向失效或错误实例新建分发；不要求撤回已经进入网络发送的消息 |
| C16 | getter 每次小于慢读阈值，但多资源累计准备较长 | 不误报单次慢读；心跳、退订及其他未满载请求仍能推进 |
| C17 | 精确订阅 getter 阻塞 | 与组合订阅一样不占管理锁或接收循环，保留精确失败语义 |
| C18 | 运行任务取消但 getter 未返回 | 不提前释放执行名额，不启动超出容量的替代任务；实际返回后释放名额 |
| C19 | 队列满时提交新请求 | 明确拒绝，连接及旧已提交订阅保留，本次预留关系回滚 |
| C20 | 同 revision 重发、同组独立等待者、已就绪组复用 | 重发不增加等待对象；独立等待者有界；复用就绪组不要求新执行名额 |
| C21 | 排队请求退订或关闭 | 移除排队工作及需求，不再执行其 getter，后续有效需求可推进 |
| C22 | 容量恢复后重发原失败版本，再发更高版本 | 原版本复用拒绝结果，更高版本可以尝试；容量恢复不自动启动失败请求 |
| C23 | 最小执行容量、多条件请求、共享准备交错 | 不因任务占名额等待同池子任务而死锁，有效请求最终可完成 |
| C24 | 非法容量配置及边界容量 | 按配置校验契约处理，实际运行与等待数量不突破上限 |

按 6.5 的有界容量和拒绝规则验收；具体配置默认值在实现时回填，并增加配置读写测试。C01 的其他请求在仍有执行容量或只需复用已有组时应可推进，不要求在所有执行名额均被阻塞 getter 占用时继续执行新 getter。C15 还须验证新请求提交前不缓存补发、旧已提交订阅不受新请求准备影响。

测试使用受控 getter 与同步信号安排顺序，不能用“取消 Task 成功”证明底层 getter 已终止。

### 11.9 ACK、异常与协议兼容

| 编号 | 场景 | 验收结果 |
|---|---|---|
| A01 | 全成功、部分失败、全跳过、零匹配 | ACK 接受且不增加观察状态字段 |
| A02 | 无效正则 | 使用现有错误路径，无新增订阅、句柄或准备状态残留 |
| A03 | 初始正则匹配超时 | 拒绝本次建立；更新场景保留旧订阅 |
| A04 | 事件分发正则超时 | 不退订，不阻断其他订阅，诊断限频 |
| A05 | 空条件、非法 PerformanceLevel | 保持现有校验，不建立观察 |
| A06 | 完整、精简传输模式 | 来源、类型、值及源端时间戳符合原协议 |
| A07 | 清理对象抛异常 | 记录诊断，继续清理其他对象，不静默宣称全部释放 |
| A08 | 单客户端发送失败、超时或中止 | 清理对应关系，不破坏健康客户端共享观察 |
| A09 | 序列化失败、消息超限、分发过载 | 保留既有恢复及限流行为 |
| A10 | ACK 与事件交错到达 | 客户端正确处理，不假定 ACK 永远先于事件 |
| A11 | 精确资源成功及失败、原生事件、Method、非 changed 条件 | 保留现有精确分支行为，不自动回退为宽松批量订阅 |

正则超时测试断言错误结果和状态清理，不要求恰好 100ms 返回；正则计算限制与慢 Get 报警分别测试，不能混用。初读具体错误信息丢失属于已记录 Core 缺陷，测试不得把完整诊断作为当前已具备能力。

### 11.10 真实端到端、重连与重叠规则

真实链路至少覆盖：

```text
客户端 Hub 订阅 → 客户端 WS 聚合 → 服务端 WS 聚合
→ Core 资源观察 → 资源变化 → WS 传输 → 客户端 Hub 回调
→ 取消订阅 → 服务端释放观察
```

| 编号 | 场景 | 验收结果 |
|---|---|---|
| E01 | 单 regex、多资源变化，分别使用完整/精简模式 | 真实观察事件到达客户端 Hub，值与源端时间戳正确，取消后释放 |
| E02 | 两个客户端共享观察并分别退出 | 分发与共享、最后持有者释放均正确 |
| E03 | 断线后重连重订阅 | 旧会话完成清理，新连接恢复观察，无多余订阅残留 |
| E04 | 重连时另一客户端仍持有同组 | 复用观察，不重新初读或重置基线 |
| E05 | 重连期间取消 | 不恢复已取消订阅，不遗留观察 |
| E06 | 修改条件、revision、性能级别后重连 | 恢复最新有效订阅状态 |
| E07 | 旧组已释放后新建组，与仍复用现有组对照 | 前者可重新尝试失败初读，后者不自动重试 |
| E08 | 两个不同 regex 同时命中同一事件 | 分别统计 Core 发布、按远端 Token 的网络发送、各本地回调次数，核对路由契约 |
| E09 | 准备超过 ACK 等待时限但未耗尽重试 | 同 revision 重发复用同一准备，getter 次数和句柄数不增加，完成后可激活 |
| E10 | 心跳正常但 ACK 重试耗尽 | 按现有策略断开并重连；旧请求失效、迟到结果清理，不因心跳正常而无限等待 ACK |
| E11 | ACK 重发期间取消或更高版本覆盖 | 旧重发不复活订阅，旧 ACK 不覆盖最新状态，任务与结果缓存有界 |
| E12 | 容量拒绝后容量恢复 | 客户端维持现有 Failed 行为，不自行重试；显式新请求可恢复 |
| E13 | 断线时旧 getter 未返回，新连接已经建立 | 总执行名额仍受约束，旧结果不提交到新会话，不无界创建阻塞工作 |

现有 `IntersectingFilters_ShouldBroadcastRemoteEventOnceAndInvokeEachLocalMatchOnce` 使用相同字面规则复用，并由测试服务端发送单条消息，不能证明 E08 的真实重叠 regex 行为。必须补充真实服务端测试。

已确认 E08 先验证现状；若暴露既有重复路由，单独记录复现、计数基线及后续设计，本阶段不顺带实现跨聚合去重，也不承诺每个本地订阅只回调一次。接入不能新增多余事件注册或额外重复分发。验收报告须区分“本次接入符合范围”和“既有路由问题仍存在”，不得隐藏问题或将仅共享轮询等同于消息去重。

### 11.11 既有测试复用与新增组织

| 现有测试文件或测试集 | 处理要求 |
|---|---|
| `WebSocketServerPhase7Tests` | 保留精确订阅、聚合、revision 测试，增加 regex 场景；未注册路径和尾随路径的本地对象由 Token 改为零观察的 ResourceGroupSubscription，保留主动事件断言 |
| `ClientSubscriptionAggregatorTests` | 补正则比较、去重、签名及字面大小写兼容 |
| `WebSocketClientAggregationTests` | 补真实不同 regex 重叠的链路验证，不能只依赖模拟单消息 |
| `WebSocketHeartbeatReconnectPhase6Tests` | 补 regex 观察恢复、慢准备期间心跳及重连取消 |
| `WebSocketServerReliabilityTests` | 回归发送异常、超时、序列化及限流，并检查共享观察保留情况 |
| `WebSocketTimestampTests` | 保留序列化测试，补轮询事件端到端时间戳 |
| Core `RegexResourceObservationTests` 及相关释放测试 | 回归底层匹配、共享、无重试、基线、超时和关闭能力 |
| Core 新增慢 Get 监测测试 | 覆盖 11.13 的监测、配置、诊断及读取语义；WS 仅补必要的链路验证 |

建议新增 `WebSocketRegexSubscriptionTests`、`WebSocketRegexSubscriptionLifecycleTests`、`WebSocketRegexSubscriptionEndToEndTests`，分别容纳功能、竞争和真实链路测试，避免将所有新增用例继续堆入 Phase7。最终文件名可随实现组织调整，但覆盖编号不可遗漏。

### 11.12 测试方法与通过标准

1. 以本节编号建立“需求 → 测试方法 → 执行结果”对应关系；合并或参数化用例时仍标明各自覆盖范围。
2. 使用计数 getter、可控值、可控失败及同步门闩安排读取和竞争；在 `finally` 解除阻塞并清理资源，禁止永久阻塞测试进程。
3. 正向接收使用事件信号或带总超时的等待；负向事件断言结合已确认的提交/清理状态和有界观察窗口，避免仅固定 Sleep 后检查。
4. 关键竞争覆盖不同完成顺序；重复执行用于发现不稳定性，不能代替受控时序测试。涉及共享 Hub 的测试按现有隔离规则执行。
5. 每次验证事件过滤与资源观察两条路径；对共享与释放检查 Token、句柄、任务和预留状态，不能只检查消息或任务数量。
6. 先运行新增及受影响测试，再运行 WS 全量回归和 Core 相关回归。实际结果中记录命令、环境、通过/失败/跳过数量及报告位置。
7. 按已确认的 revision、容量、ACK 和慢读规则验收；E08 按独立记录既有问题的范围处理。未执行项目标记未验证，配置默认值及限频参数由实现回填，不得将计划当作通过结果。
8. 本次接入引入的失败必须修复；既有缺陷与同步 getter 取消限制明确记录影响和边界，不能用跳过测试或改弱断言隐藏。

本节最初作为测试计划编写；本次执行结果和覆盖对应已回填第 13 节。覆盖编号是需求点，多个编号可由同一测试与 Core 回归共同验证，不等同于独立测试方法数量。

### 11.13 慢 Get 报警专项测试

以下是本次 Core 配套改动新增的必测项，需同时检查监测和读取语义，不能只检查日志文本。

| 编号 | 场景 | 验收结果 |
|---|---|---|
| G01 | 默认阈值、配置覆盖及读写 | 默认 100ms，正有限配置生效；非法值按明确校验规则处理，独立于正则超时 |
| G02 | 阈值以下、恰达阈值、超过阈值 | 用可控单调时间验证边界，达到阈值即符合慢读条件，不用真实 Sleep 判定精确边界 |
| G03 | 组合初读仍阻塞，监测超过阈值 | 在释放 getter 前已经观察到 Error，包含路径、初读阶段及耗时信息 |
| G04 | 轮询读取仍阻塞 | 同样可在返回前发现 Error，轮询阶段正确，无新增同资源并发读取 |
| G05 | 本地精确 SubscribeResource 初读慢 | 通过统一设施报警，不漏掉管理器之外的精确初读，不重复执行 Get |
| G06 | getter 超阈值后返回，但监测回调尚未执行 | 返回路径仍报告慢读；迟到回调不产生独立重复告警或关联到下一次读取 |
| G07 | 报警后返回有效结果 | 记录最终耗时；初值仍作基线，轮询变化正常发布，不因慢读标记 Failed 或停止观察 |
| G08 | 慢读后抛异常或返回无效 Sample | 报警与原失败处理并存，监测清理；组合初读不重试，精确初读仍失败，运行期行为保持 |
| G09 | 等锁、排队或正则匹配耗时长，但 Get 本身快 | 不误记为 getter 慢读；无实际 Get 的共享句柄路径不登记读取监测 |
| G10 | 一个资源持续慢、多资源分别慢、恢复后再次慢 | 按资源限频且可区分来源，保留抑制计数或汇总，不刷屏也不永久隐藏后续异常 |
| G11 | Get 完成与监测触发同时发生 | 同次状态转换一致、记录可关联，无错误归属、未观察异常或残留记录 |
| G12 | 正常返回、失败、退订、模块关闭及迟到返回 | 已完成记录最终清理；监测不阻塞关闭、不复活请求；未结束读取按已知边界保留且最终可回收 |
| G13 | 同资源共享、不同资源并发、连续多次读取 | 一次物理读取对应一份监测，不按订阅者重复计时，旧读取记录不覆盖新读取 |
| G14 | 监测设施工作量、时钟及日志回调 | 不为每次 Get 分配阻塞等待线程；墙钟变动不影响耗时；日志不持有读取或 WS 管理锁，监测与业务关闭无死锁 |

监测边界、限频及竞争以可控时钟/调度器做确定性测试，同时保留实际阻塞 getter 的集成测试验证“未返回也会报警”。不要求真实调度在第 100ms 精确输出，不以永不释放的 getter 测试卡死。Core 慢读测试、WS 慢准备测试和 ACK 重发测试需分别通过。

### 11.14 已确认决策与覆盖对应

| 决策 | 主要测试编号 |
|---|---|
| 100ms 慢 Get 报警，结果及生命周期保持 | G01—G14、O03—O06、C13、C18 |
| 精确及组合订阅异步准备，控制消息不阻塞 | C01—C17、C23、A11 |
| 保留 ACK 超时重发和耗尽后重连 | E09—E11、E13、R01 |
| 最高请求版本与最后提交状态分离，失败结果幂等 | R01—R17、C09—C12 |
| 有限执行和等待容量，满载拒绝且不自动重试 | C18—C24、E12—E13 |
| 重叠 regex 路由先验证、既有问题独立处理 | E08、L04、F05 |
| 正则仅展开资源，Get 在来源端执行 | F01—F02、F12、L07、P01—P06 |

原有功能矩阵继续有效，本表只核查新增决策的覆盖，不替代完整矩阵。测试项数量代表计划覆盖点，不代表已经存在同等数量的测试方法。

## 12. 实施顺序与完成标准

1. 修正条件比较、去重及签名，补充客户端和服务端测试。
2. 增加 Core 统一慢 Get 监测与配置，通过初读、轮询和精确订阅兼容测试。
3. 建立准备/提交、请求失效、有界容量和锁外清理机制，按已确认 revision 策略验证慢 getter 与关闭竞争。
4. 接入 SubscribeResources，绑定具体聚合实例，保留精确资源分支。
5. 完成事件范围、资源共享、参数限制、失败、无重试及 ACK 协议重发测试。
6. 执行 WS 全量回归、Core 相关回归和真实连接测试，确认协议、时间戳及精确订阅兼容；E08 发现的既有路由问题独立记录。
7. 将实际修改、配置默认值、限频策略、执行命令、测试结果和剩余限制回填文档。

完成标准是功能、生命周期和兼容性测试通过，并明确同步 getter 的取消限制；不能仅以替换一次 API 调用或收到一次 changed 作为完成依据。

## 13. 本次实现与验证记录（2026-10-09）

### 13.1 实际实现与配置

- 非精确来源使用 `SubscribeResources`，保存 `ResourceGroupSubscription`；精确来源仍使用 `SubscribeResource`。
- 服务端登记请求后，在有界后台工作中执行 Core 初读；准备状态、最后提交关系和最高请求版本分别管理。同组共享准备，同 revision 重发共享结果，新版本转移仍需要的准备引用。
- 退订、会话关闭、更新覆盖使在途请求失效；迟到对象释放。读取尚未结束时执行名额不释放。就绪组可在准备满载时直接复用。
- Session 跟踪 ACK 任务，不为同一在途重发追加等待；发送前再次检查请求身份，ACK 发送超时/失败清理会话。
- 聚合回调绑定实例；Source / EventType 按字面或原始正则文本比较，签名使用长度编码，客户端同步修正。
- Core 初读与轮询使用共享慢读监测器，单调计时、25ms 检查周期；日志在读取调用之外输出，不占设备读取锁。

| 配置 | 默认值 | 说明 |
|---|---|---|
| `GlobalConfig.ResourceSlowGetThresholdMs` | 100 | 正整数毫秒；Set/Populate 中缺失、非整数或非正数回退 100，Get 可导出 |
| `MaxConcurrentSubscriptionPreparations` | 4 | 每个 WS 服务端同时执行的 Core 准备上限，必须大于 0 |
| `MaxPendingSubscriptionPreparations` | 256 | 排队准备上限，必须非负；独立未完成请求同时受“执行上限 + 等待上限”约束，默认 260 |
| `SubscribeAckTimeout` / `MaxSubscribeRetryCount` | 10 秒 / 3 | 沿用原配置与重试耗尽后重连语义 |

容量满的 Reason 为 `subscriptionPreparationCapacityExceeded`，连接保留；同 revision 重发复用拒绝结果。Core 慢读按资源每秒最多选择一次读取发详细告警及其完成耗时，其余以 suppressed 汇总；每次告警有独立 read 编号。检测与日志受实际线程调度影响，不保证精确第 100ms 输出。

### 13.2 测试执行结果

环境：Windows、.NET SDK 10.0.401；Core 目标 netstandard2.0，测试目标 net10.0。以下命令在工作区根目录执行：

```powershell
dotnet test websocketcommunicationmodule/TestCode/TestCode.csproj --no-restore --logger "trx;LogFileName=ws-coverage-full.trx" --results-directory .codex-test/coverage-completion
dotnet test newcfet2core/Code/CodeTest/CodeTest.csproj --no-restore --logger "trx;LogFileName=core-coverage-full.trx" --results-directory .codex-test/coverage-completion
```

| 测试集 | 通过 | 失败 | 跳过 | 结果文件 |
|---|---:|---:|---:|---|
| WS 全量 | 237 | 0 | 0 | [ws-coverage-full.trx](../../.codex-test/coverage-completion/ws-coverage-full.trx) |
| Core 全量 | 195 | 0 | 0 | [core-coverage-full.trx](../../.codex-test/coverage-completion/core-coverage-full.trx) |

首次实施验证为 WS 222、Core 185 项通过。2026-10-09 覆盖复查后，本轮新增 15 个 WS、10 个 Core 测试结果（含参数化数据行），并加强既有断言；相对实施前基线累计新增 53 个 WS、21 个 Core 测试结果。原未注册路径/尾随路径测试调整本地组合对象类型；原先失败后同 revision 改换资源的测试改用更高 revision，以符合已确认契约。本轮仅修改测试和本文档，未修改生产逻辑。

构建仍有原有弃用/可空性/分析器告警，以及 NuGet 漏洞索引不可访问的 NU1900 提示；无编译错误。本次没有发布包或提交 Git。

### 13.3 覆盖对应与证据

下表是测试集导航，不代表整段编号中的每个子场景都有独立端到端断言。本轮复查发现的缺口及补充证据逐项列在 13.3.1；测试通过数量不能代替场景覆盖判断。

| 验收范围 | 主要测试文件与方法（可组合覆盖） |
|---|---|
| F01—F12、O01—O09：匹配、初值、主动事件、部分失败、无重试及固定目录 | WS `RegexObservesMultipleResourcesAndForwardsActiveUnregisteredAndNativeEvents`、`RegexAlarmAndParameterizedPathsRemainEventOnlyAndPairedRulesDoNotCrossMatch`、`PartialFailureSkippedAndZeroMatchesDoNotRetryOrRejectEventSubscription`；Core `RegexResourceObservationTests` 全集 |
| P01—P06：参数资源及正则边界 | `ParameterizedResourcesRespectExactAndRegexObservationBoundaries` 的 6 个数据行，以及主动参数路径事件测试 |
| L01—L11：共享与释放 | `RegexGroupsShareHandlesWithLocalAndExactSubscriptionsAndReleaseIndependently`、`TwoSessionsShareRegexAndReconnectReusesSurvivingObservation`、`UnsubscribeThenReuseTokenDoesNotLetOldWorkerRemoveNewSubscription`；既有 Phase7 聚合及 Core 基线/释放测试 |
| K01—K07：条件语义和签名 | `RegexCaseEscapesAndSignatureBoundariesRemainDistinctButLiteralCaseShares`、`ClientDoesNotFoldRegexEscapesOrMergeSignatureSeparators`；既有大小写、重复规则回归及 Core 完整匹配测试 |
| R01—R17：revision、失败缓存和回滚 | `HigherFailedRevisionKeepsCommittedSubscriptionAndNeverRestoresOlderPreparation`、`PerformanceUpdateDuringPreparationTransfersReservationWithoutAnotherRead`、`MultiRuleFailureRollsBackOnlyNewGroupsAndPreservesSharedAndCommittedGroups`、`RegexInitialTimeoutCachesFailureAndPreservesPreviouslyCommittedSubscription`；既有客户端条件拆分、ACK 版本过滤和 Phase7 更新测试 |
| C01—C24：准备、退订、关闭、名额和竞争 | `WebSocketRegexSubscriptionLifecycleTests`；真实连接 `RegexPreparationDoesNotBlockPingAndRetransmissionsDoNotReadAgain`、`SlowExactOrRegexCanBeUnsubscribedBeforeInitialReadReturns`；Core 初始化取消/关闭与读取串行回归 |
| A01—A11：协议和异常 | 既有 Phase2/Phase7/Reliability/Timestamp 全量；新增 `Session_SubscribeAckSendTimeoutShouldDisposeAndReleaseSubscription`、释放抛异常后继续清理、初始正则超时测试；Core 分发超时隔离回归 |
| E01—E13：真实 WS、客户端 Hub、重连和重复事件 | `WebSocketRegexSubscriptionEndToEndTests.cs` 中真实服务端、客户端模块及 loopback Hub 测试；既有 Phase6 重连、取消和最新性能级别回归 |
| G01—G14：慢 Get | `SlowResourceReadTests` 的可控单调时间、阈值边界、限频、异常清理、完成/监测竞争及自动监测测试；`SlowResourceReadIntegrationTests` 的组合初读、精确初读、轮询 3 个真实阻塞读取场景；Core 共享与无重试回归 |

测试不逐一复制 124 个矩阵编号为独立方法：底层行为由 Core 回归验证，WS 测试验证接入及传输结果；线程设施、锁范围、计时位置、签名编码等同时进行代码结构复查。实际硬件延迟分布、长时间吞吐和系统线程饥饿不属于本轮自动化实测，不将测试通过解释为硬实时保证或生产负载容量结论。

#### 13.3.1 覆盖复查后的补充证据（2026-10-09）

WS 管理器补测见 [WebSocketRegexCoverageTests.cs](../TestCode/WebSocketRegexCoverageTests.cs)，真实网络补测见 [WebSocketRegexCoverageEndToEndTests.cs](../TestCode/WebSocketRegexCoverageEndToEndTests.cs)。Core 补测见 [SlowResourceReadCoverageTests.cs](../../newcfet2core/Code/CodeTest/Event/SlowResourceReadCoverageTests.cs)。下面“已覆盖”仅指该行写出的具体断言，不扩大为整个编号的所有组合排列均已实测。

| 编号/具体场景 | 状态 | 测试方法及关键断言 |
|---|---|---|
| C09，同生命周期中新版本先成功、旧 getter 后返回 | 已覆盖 | `NewRevisionCommitsBeforeOldGetterReturnsAndSurvivesLateCleanup`：新组先提交；旧任务取消；迟到清理不替换新实例；最后退订计数归零 |
| R09，更新后的组合部分资源初读失败 | 已覆盖 | `RevisionWithPartiallyUnreadableResourcesCommitsAndDoesNotRetry`：ACK 成功；结果包含 Failed/Started；释放旧组；后续性能更新不重新读取失败资源 |
| R15，失败缓存后退订或会话清理 | 已覆盖 | `FailedRevisionCacheIsRemovedAtSubscriptionOrSessionEnd` 两个数据行：旧失败任务复用；生命周期结束后相同版本创建新任务并重新初读 |
| R16，本地多条件拆分后一成功一失败 | 已覆盖 | `ClientSplitRulesCommitSuccessIndependentlyOfRejectedRuleAndReleaseBoth`：实际客户端模块同时 Active/Failed；成功项收到变化值；取消后两项状态及服务端观察释放 |
| R17，更新拒绝、旧关系继续、旧 ACK 迟到 | 已覆盖（网络服务端 + 客户端状态存储分层） | `RejectedUpdateKeepsOldServerEventsAndClientFailureSurvivesOldAck`：使用真实 ACK 驱动 `RemoteEndpointSubscriptionStore`；最新版本保持 Failed；旧成功 ACK 不覆盖；旧订阅仍收到事件。此用例不宣称通过完整客户端自动更新流程 |
| E13，旧 getter 未返回时新连接已建立 | 已覆盖 | `ReconnectWhileOldGetterIsBlockedKeepsCapacityAndCannotCommitOldSession`：主动断线后新连接握手；同 Token 排队；执行名额仍为 1，超额请求拒绝；释放旧读取后只有新会话提交。自动 ACK 耗尽重连另由原用例验证 |
| C13，取得部分句柄后阻塞再取消 | 已覆盖 | `CancellingPartiallyPreparedBatchRetainsHandlesOnlyUntilBlockedGetterReturns`：取消后临时保留已取得句柄；实际返回后观察、事件订阅、执行及等待计数归零 |
| C16，多次短读取累计准备较长 | 已覆盖 | `ManyShortReadsHaveLongPreparationWithoutSlowGetAlarmAndKeepControlResponsive`：50 个 getter 各延迟 20ms，阈值配置 500ms；累计超过阈值但无慢读报警；期间 Pong、其他请求、退订推进；无迟到 ACK。使用放大时间预算避免依赖精确 100ms 调度 |
| A10，事件先于 ACK | 已覆盖 | `EventBeforeSubscribeAckIsDeliveredWhilePendingAndThenBecomesActive` 两个性能级别：测试服务端先发事件并暂停 ACK；确认 Pending 状态已收到正确值，再释放 ACK 转为 Active |
| A07，清理抛异常的诊断 | 已覆盖 | `OldAggregateCallbackCannotReachReplacementAndCleanupContinuesAfterFailure` 增加错误内容和次数断言，同时检查其他组清理继续 |
| E01，多资源真实客户端 Hub 回调 | 已覆盖 | `RegexLoopbackTraversesClientAggregatorAndReturnsTimestampToHub` 两个性能级别加强为 Temperature/Pressure 两资源，分别检查值、来源、类型、远端标记、时间戳及退订释放 |
| E04/L07，重连共享观察、不初读、不重置基线 | 已覆盖 | `TwoSessionsShareRegexAndReconnectReusesSurvivingObservation`：阻塞现有轮询，再改变值并重连；同一观察对象、读取次数不增加；解除阻塞后存续与重连客户端均收到变化值 |
| K02/K03，转义大小写的实际事件匹配 | 已覆盖 | `RegexEscapeCasesActuallyFilterEvents` 参数化验证正反匹配；原客户端/服务端聚合测试继续检查不合并 |
| K05，文本不同但语义等价的 regex | 已覆盖（服务端/Core） | `EquivalentRegexTextKeepsSeparateGroupsButSharesObservation`：两个聚合组、一次初读、一个 Core 观察；各组独立释放 |
| K06，客户端签名边界 | 已覆盖 | `ClientDoesNotFoldRegexEscapesOrMergeSignatureSeparators` 增加真正含 `\0` 的来源/类型分隔案例；服务端既有签名碰撞测试保留 |
| K07，已注册路径含正则元字符 | 已覆盖 | `RegisteredMetacharacterPathUsesExactObservationAndExistingFilterSemantics`：已注册 `/a.b/Value` 仍走精确资源初读，只观察该资源；主动事件过滤保留点号的正则语义；显式转义后限制为字面点号 |
| P06，默认参数的实际取值 | 已覆盖 | `ParameterizedResourcesRespectExactAndRegexObservationBoundaries` 增加默认 channel 确为 7 的断言 |
| G01，文件 Populate 配置入口 | 已覆盖 | `SlowThresholdFilePopulateUsesSameValidationAndDefaults`：临时 JSON 文件验证有效、缺失、非法、非正值，最终恢复全局配置 |
| G08，慢读后抛异常/无效 Sample | 已覆盖 | `SlowThrowOrInvalidSamplePreservesInitialAndPollingFailurePolicy` 六个数据行：组合初读、精确初读、轮询 × 异常/无效样本；先报警后解除阻塞；初读不重试、精确失败、轮询保留并恢复；监测最终清理 |
| G12，轮询阻塞时退订/关闭及迟到返回 | 已覆盖 | `BlockedPollingReadKeepsOneMonitorUntilLateReturnAfterUnsubscribeOrShutdown` 两个数据行：关闭及时返回；物理读取未结束时保留一条监测；迟到返回清理且不发布旧值 |
| G09/G13，等待、共享及不同资源并发 | 已覆盖（实际读取边界） | `WaitingAndSharedAcquisitionsDoNotRegisterAdditionalPhysicalReads`：同资源等待者不增加监测；两个不同资源各一条监测；共享句柄不触发初读或新监测；释放后归零 |

证据边界：排队/正则匹配不计入 Get 耗时，以及共享定时器不为每次 Get 新建线程，除上述行为测试外仍依赖计时位置和线程结构复查；未新增系统调度压力/长期运行基准。E11 中已明确忽略的 ACK 积压与失败请求容量保护问题不计为本轮通过项，重叠 regex 重复分发继续按现状测试记录，不修复。

### 13.4 已确认的剩余限制与独立问题

1. **同步 getter 不能强制中断**：请求失效后迟到结果只清理；永不返回时仍占用名额及准备期资源。100ms Error 不等于 100ms 自动结束读取。
2. **重叠 regex 既有双重分发已复现**：`OverlappingRegexWireBaselineSendsOneMessageForEachRemoteToken` 验证两个远端 Token 各收到一条；`OverlappingRegexRealClientRecordsExistingDoubleDispatchBaseline` 验证一次源事件经两个匹配组返回后，每个匹配的本地订阅回调两次。这是既有“远端消息重新进入全部匹配 Hub 过滤器”的路由问题，本次按已确认范围独立记录，未增加去重策略。该测试记录现状，不表示认可 exactly-once 保证。
3. **参数化自动观察仍不支持**：必填参数跳过或精确初读失败；路径 regex 不枚举参数，默认参数仅观察默认读取。
4. **Core 初读错误详情丢失仍待修复**：无效 Sample 的通用原因未在本次扩展；慢读日志提供耗时信息，不补回 Sample 的设备错误详情。
5. **ACK 等待预算仍可能触发重连**：心跳正常也不改变既有策略；满载拒绝后客户端不会因容量恢复而自行重试。

## 2026-10-12 参数化订阅更新

按 [订阅使用设计文档](../../newcfet2core/Code/CFET2Core/doc/订阅使用/订阅使用设计文档.md) 执行：所有声明参数的 getter 均禁止自动观察，包括默认参数和空 params。WS 基础含参数 CR 走组合事件分支，不执行 SubscribeResource 初读；详细路径及正则仍只接收参数化主动事件。无参数精确资源使用 SubscribeResource 的字面来源过滤，注册路径中的点号不再误匹配其他路径；显式正则保持原规则。

新增 12 项 WS 参数分支/载荷用例，更新旧必填参数拒绝、默认参数观察和元字符路径用例；部分失败清理测试改用真正初读失败的无参数资源。2026-10-12 WS 全量 265 项通过，无失败或跳过。前文数字及旧规则属于对应阶段记录。
