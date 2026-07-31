# CFET2 WebSocket Communication Module

CFET2 的远程事件通信模块。它通过 WebSocket 实现远程订阅、事件回流、取消订阅、心跳和重连，并支持客户端与服务端订阅聚合。

## 功能

- 注册 `ws` 与 `http` 协议；`http://host:port` 映射到 `ws://host:port+1` 的事件端点。
- 复用同一远端 endpoint 的客户端连接。
- 支持订阅确认、失败重试、取消订阅、心跳、超时和重连。
- 按 endpoint/source/event type 聚合客户端订阅，并在服务端复用本地 EventHub 订阅后 fan-out。
- 支持 `PerformanceLevel` 0 和 1。
- 客户端可携带 `Authorization` 请求头和 hello 鉴权字段；服务端当前仅匿名接受连接，尚未实现认证或鉴权。
- 提供连接/订阅状态 DTO，以及公开只读的 `WebSocketCMThing` 诊断 Thing。

## 快速开始

安装依赖：

```powershell
dotnet add package CFET2Core --version 3.0.0
dotnet add package Jtext103.CFET2.WebSocketCommunicationModule --version 1.0.0
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
