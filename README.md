# CFET2 WebSocket Communication Module

CFET2 的远程事件通信模块。它通过 WebSocket 实现远程订阅、事件回流、取消订阅、心跳和重连，并支持客户端与服务端订阅聚合。

## 功能

- 注册 `ws` 与 `http` 协议；HTTP 事件订阅由 Core 的 HttpEventSubscriptionTransport 选择模块；选择 WebSocket 时，`http://host:port` 映射到 `ws://host:{port + WebSocketPortOffset}` 的事件端点，偏移默认 1。
- 复用同一远端 endpoint 的客户端连接。
- 支持订阅确认、失败重试、取消订阅、心跳、超时和重连。
- 按 endpoint/source/event type 聚合客户端订阅，并在服务端复用本地 EventHub 订阅后 fan-out。
- 支持 `PerformanceLevel` 0 和 1：0 传输完整 EventArg，1 传输 Source、EventType、Value、Timestamp。等级不参与客户端聚合键，组内按最小等级决定远端载荷，并在成员变化时更新。
- 服务端无参数精确资源订阅复用 Core 的标量及受支持容器自动观测规则；不支持的自动观测类型返回带原因的失败 ACK。正则组合逐资源跳过不合格观察；含参数 getter 不轮询，WS 基础含参数来源和详细事件路径走纯事件订阅，Core 远程 SubscribeResource 的初值由资源模块读取。原生事件复杂资源仍可主动发布。详见 [Core 类型边界](../newcfet2core/README.md#资源自动观测的类型边界)。
- 客户端收到当前订阅的失败 ACK 后，保存 `Failed/LastError` 并记录 Error 日志，包含远端、资源/事件、token、revision 和原因。同一次失败不重复输出，过期或未知 token 的 ACK 不报错。CFET2APP 默认同时输出到控制台及程序目录的 `Log_yyyy-MM-dd.txt`；自定义宿主按其日志 Provider 配置输出。
- 再次订阅同一失败聚合组且无需变更远端订阅时，每次调用都会输出已保存的失败原因，不重新发送 Subscribe。重复 ACK 去重与用户再次调用订阅分别处理。
- 内部统一使用 `WebSocketLog.Logger`，默认是无输出的 EmptyLogger；WebSocketCM 构造时切换为 `Cfet2LogManager.GetLogger("WebSocket")`。宿主应先配置日志 Provider，再创建模块。内部对象不缓存 Logger，初始化前创建的对象也会在后续调用时使用新 Logger；初始化前的日志不补写。同一进程中的 WebSocketCM 共用该入口，内部组件构造函数不再逐层接收 logger。
- 客户端可携带 `Authorization` 请求头和 hello 鉴权字段；服务端当前仅匿名接受连接，尚未实现认证或鉴权。
- 提供连接/订阅状态 DTO，以及公开只读的 `WebSocketCMThing` 诊断 Thing。

## 快速开始

安装依赖：

```powershell
dotnet add package CFET2Core --version 3.1.0
dotnet add package Jtext103.CFET2.WebSocketCommunicationModule --version 1.0.1
```

注册模块和诊断 Thing：

```csharp
using Jtext103.CFET2.WebSocketCommunicationModule;
using Jtext103.CFET2.WebSocketCommunicationModule.Diagnostics;
using Jtext103.CFET2.WebSocketCommunicationModule.Options;

var webSocketCm = new WebSocketCommunicationModule(new WebSocketCommunicationOptions
{
    EnableServer = true,
    ListenUri = new Uri("http://0.0.0.0:8002/")
});

host.MyHub.TryAddCommunicationModule(webSocketCm);
host.MyHub.TryAddThing(new WebSocketCMThing(webSocketCm), "/", "websocketcm");
```

## 配置与扩展

- `WebSocketCMThing` 提供 `status`、`serverSessions`、`remoteClients` 和 `remoteClient` 等只读 Status。
- `status.State` 为 Created/Started/Disposed；Started 不代表远端订阅已确认。通过 `RemotePendingSubscriptionCount`、`RemoteActiveSubscriptionCount`、`RemoteFailedSubscriptionCount` 及远端明细判断进度和失败。
- 模块 `ResubscribeInterval` 默认 50 ms；CFET2APP 通过 `WebSocketResubscribeIntervalMs` 覆盖为默认 10 ms（允许 0），并默认将诊断 Thing 挂载为 `/WebSocketCM`。
- 状态查询不会创建连接、触发重连或修改订阅状态；对 endpoint、会话和订阅信息的访问应由上层控制权限。
- `WebSocketCommunicationOptions.Auth` 仅控制客户端请求头与 hello 字段的透传；服务端不校验 scheme 或 token。`IWebSocketAuthenticator` 目前是空的预留扩展接口。
- 不要在配置或日志中暴露 token。
- 源码测试需要 .NET 10 SDK：`dotnet test .\TestCode\TestCode.csproj`。

## 文档

- [实现说明与测试边界](<Doc/WebSocketCM 实现说明与测试边界 V1.0.md>)
- [具体实现](<Doc/WebSocketCM具体实现.md>)
- [类职责分析](<Doc/WebSocket类职责分析.md>)
- [PerformanceLevel 设计](<Doc/PerformanceLevel实现文档.md>)
- [WebSocketCMThing 设计](<Doc/WebSocketCMThing设计文档.md>)
- [历史方案](Expired/)
