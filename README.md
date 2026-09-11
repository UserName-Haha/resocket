# ws-market-client

[![CI](https://github.com/UserName-Haha/ws-market-client/actions/workflows/ci.yml/badge.svg)](https://github.com/UserName-Haha/ws-market-client/actions/workflows/ci.yml)
[![JitPack](https://jitpack.io/v/UserName-Haha/ws-market-client.svg)](https://jitpack.io/#UserName-Haha/ws-market-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

基于 OkHttp 的 WebSocket 长连接客户端：心跳、退避重连、重连后自动恢复订阅，消息以 Kotlin Flow 暴露，背压策略可配置。

## 解决什么问题

行情、IM 这类高频推送场景，OkHttp 只给了一条连接，剩下的每个团队都要自己写一遍：

- 弱网下连接"半开"，本端以为连着，实际早已不通，没有任何回调，只能靠心跳发现；
- 断线重连要退避，重连后要把之前的订阅补发一遍；
- 手动断开和自动重连的竞态、旧连接迟到的回调、订阅消息和连接成功之间的时序；
- 行情一秒几百条，界面处理不过来时要么内存一直涨，要么卡住读线程把连接拖死；
- 页面销毁时忘了退订，服务端一直推没人要的数据。

这些问题都不难，但每一个都很容易写错，而且平时测不出来。这个库把它们一次做对，并且用单测固定下来。

库不理解任何具体协议：订阅消息长什么样、消息属于哪个 topic、心跳发什么，由你通过配置告诉它。

## 快速开始

在 `settings.gradle.kts` 里加上 JitPack：

```kotlin
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven("https://jitpack.io")
    }
}
```

添加依赖：

```kotlin
dependencies {
    implementation("com.github.UserName-Haha:ws-market-client:0.1.0")
}
```

连接并订阅：

```kotlin
val client = WsClient.create(okHttpClient) {
    url("wss://stream.example.com/ws")
    topicOf = { message -> /* 从消息里取出 topic，取不到返回 null */ }
}
client.connect()

val trades = Subscription(
    topic = "btcusdt@trade",
    subscribeMessage = """{"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":1}""",
    unsubscribeMessage = """{"method":"UNSUBSCRIBE","params":["btcusdt@trade"],"id":2}""",
)

viewModelScope.launch {
    client.subscribe(trades).collect { message -> /* 原始消息字符串 */ }
}
```

开始收集时订阅，收集被取消时退订，断线重连后自动恢复，都不需要再写代码。
默认每 15 秒一次协议层 ping，断线后从 1 秒开始指数退避重连，上限 30 秒。

最低支持 Android 5.0（API 21）。依赖只有 OkHttp（4.12 及以上，兼容 5.x）和 kotlinx-coroutines。不需要额外的混淆配置。

## 使用

### 订阅跟着收集走

`subscribe()` 返回的是冷流。同一个 topic 有多个收集者时只向服务端订阅一次，最后一个离开才退订。
配合 `stateIn(WhileSubscribed)`，界面退到后台就自动退订，回来再订阅：

```kotlin
val price: StateFlow<String?> = client.subscribe(ticker, Backpressure.latest())
    .map { parsePrice(it) }
    .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), null)
```

断线期间 Flow 不结束也不报错，只是暂时没有数据。需要感知断线的场景（比如要重建本地订单簿）观察 `state`：

```kotlin
client.state.collect { state ->
    when (state) {
        ConnectionState.Connected -> { /* 重新拉一次快照 */ }
        is ConnectionState.WaitingToReconnect -> { /* state.attempt、state.delay、state.cause */ }
        else -> Unit
    }
}
```

### 背压

每个收集者有独立的缓冲区，一个慢的收集者不影响其他人。

| 策略 | 行为 | 适合 |
|---|---|---|
| `Backpressure.dropOldest(64)` | 满了丢最旧的，默认 | 逐笔成交、深度增量 |
| `Backpressure.dropNewest(64)` | 满了丢新到的 | |
| `Backpressure.latest()` | 只保留最新一条 | 最新价这类只关心当前值的数据 |
| `Backpressure.unbounded()` | 不丢，缓冲区无上限 | IM 消息；收集者长期跟不上会耗尽内存 |

丢弃会通过 `WsEvent.MessagesDropped` 上报，同一个 topic 每秒最多一次。

### 心跳

```kotlin
// 默认：WebSocket 协议层的 ping 帧，服务端按协议自动应答
heartbeat = Heartbeat.protocolPing(interval = 15.seconds)

// 服务端要求特定格式的心跳：发出后 timeout 内收到任何数据都算连接活着
heartbeat = Heartbeat.text(interval = 15.seconds, timeout = 10.seconds) { """{"op":"ping"}""" }

// 服务端主动发心跳、要求客户端应答
autoReply = { message -> if (isServerPing(message)) buildPong(message) else null }
```

### 重连

```kotlin
reconnectPolicy = ReconnectPolicy.exponentialBackoff(
    initialDelay = 1.seconds,
    maxDelay = 30.seconds,
    multiplier = 2.0,
    jitter = 0.2,        // 延迟上下浮动 20%，避免服务端重启后所有客户端同时连回来
    maxAttempts = null,  // 一直重试
)

// 或者自己决定：鉴权失败就不要再连了
reconnectPolicy = ReconnectPolicy { attempt, cause ->
    if (cause is DisconnectCause.Closed && cause.code == 4001) null else (attempt * 2).seconds
}
```

库不监听网络状态。网络恢复或 App 回到前台时调用 `client.reconnectNow()`，跳过剩余的退避等待。

### 其他配置

```kotlin
WsClient.create(okHttpClient) {
    url { autoHost.rewrite("wss://ws.example.com/stream") }   // 每次连接前调用，可以在这里换线路或带上新的签名
    greeting = { listOf(buildLoginMessage()) }                // 每次连上后、恢复订阅之前发送
    binaryDecoder = { bytes -> gunzip(bytes) }                // 二进制帧解码成文本，不设置则忽略二进制帧
    configureRequest = { it.header("X-Token", token) }
    stableAfter = 10.seconds                                  // 连接保持这么久才把重连计数清零
    backpressure = Backpressure.dropOldest()                  // 默认背压策略
    listener = WsListener { event -> Log.d("WsClient", event.toString()) }
}
```

`client.messages` 是全部消息的流，包括不属于任何订阅的（订阅确认、心跳应答）。
`client.send(text)` 在未连接时返回 false，不会留到重连后补发。

## 设计说明

完整的说明在 [docs/design.md](docs/design.md)，这里列几个主要的取舍。

**状态只在一个协程里修改。** 调用方的操作、OkHttp 的回调、定时器到期都变成命令排进队列逐条处理，连接状态、重连计数、订阅表不需要锁。
"订阅消息一定在连接成功之后发出""断开之后到达的连接成功一定被忽略"这类时序自然成立。
每次发起连接递增一个代数，旧连接迟到的回调带着旧代数，直接丢弃。

**消息分发不走队列。** 消息的频率比状态变化高几个数量级。分发留在 OkHttp 的读线程上，只读两个不可变快照，订阅变化时整体替换。

**topic 由接入方从消息里取，每条消息只取一次。** 另一种做法是每个订阅带一个 `matches(message)`，
那样每条消息要被所有订阅各判断一次，每次很可能都要解析一遍 JSON。

**没有"挂起发送方"的背压策略。** 发送方是 OkHttp 的读线程，挂起它等于停止读 socket，pong 也收不到，连接会被判死。

**心跳判活看的是"有没有数据进来"。** 行情推送本身就能证明连接是通的，库不需要理解各家的 pong 格式。
只认心跳发出之后收到的数据。

**重连计数在连接稳定后才清零。** 在连上的那一刻清零的话，"能连上但马上被踢"会让退避永远停在第一档。

**不缓存发送失败的消息。** 对交易指令这类消息，延迟送达比送达失败更危险。

## 局限

- 只处理文本消息。二进制帧要通过 `binaryDecoder` 转成文本。
- `greeting` 发出后不等应答就恢复订阅。需要"登录成功后才能订阅私有频道"的协议，目前要自己在登录应答后再 `subscribe`。
- 重连后的 Flow 里没有"这里断过"的带内标记，需要的话观察 `state`。
- 不监听网络状态，不感知前后台，由接入方调用 `reconnectNow()` / `disconnect()`。
- `topicOf`、`autoReply`、`binaryDecoder` 在 OkHttp 的读线程上执行，写得太重会拖慢整条连接。
- Kotlin 优先，从 Java 调用不方便。

## 路线图

- 订阅和登录的应答确认
- Flow 里的断线标记
- 拆出不依赖 Android 的纯 JVM artifact
- 发布到 Maven Central

## Sample

`sample` 模块接的是 Binance 公开行情流：四个交易对的最新价（`latest` 背压）、选中交易对的逐笔成交（有界缓冲）、
界面退到后台自动退订、模拟断线观察重连和订阅恢复、完整的事件日志。

```
./gradlew :sample:installDebug
```

## License

[MIT](LICENSE)
