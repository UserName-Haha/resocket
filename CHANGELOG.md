# 更新日志

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循[语义化版本](https://semver.org/lang/zh-CN/)。
1.0 之前，次版本号的升级可能包含不兼容的变更，会在这里写明。

## 0.1.0 - 2026-09-19

首个版本。

- `ReSocket`：基于 OkHttp 的 WebSocket 客户端，连接状态通过 `StateFlow<ConnectionState>` 暴露。
- 心跳：协议层 ping 帧、应用层文本心跳，以及对服务端心跳的自动应答。
- 重连：带抖动的指数退避，`ReconnectPolicy` 可替换；连接稳定后才清零重连计数。
- 订阅：`subscribe()` 返回冷流，收集即订阅、取消即退订，同一 topic 引用计数；重连后自动恢复。
- 背压：每个收集者独立缓冲，`dropOldest` / `dropNewest` / `latest` / `unbounded`，丢弃通过事件上报。
- `ReSocketEvent`：可插拔的事件回调，默认静默。
