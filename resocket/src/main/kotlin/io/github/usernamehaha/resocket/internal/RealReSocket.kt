package io.github.usernamehaha.resocket.internal

import io.github.usernamehaha.resocket.Backpressure
import io.github.usernamehaha.resocket.ConnectionState
import io.github.usernamehaha.resocket.DisconnectCause
import io.github.usernamehaha.resocket.Heartbeat
import io.github.usernamehaha.resocket.HeartbeatTimeoutException
import io.github.usernamehaha.resocket.Subscription
import io.github.usernamehaha.resocket.ReSocket
import io.github.usernamehaha.resocket.ReSocketConfig
import io.github.usernamehaha.resocket.ReSocketEvent
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.ComparableTimeMark
import kotlin.time.TimeSource
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import okio.ByteString

/**
 * 连接状态、重连计数、订阅表只在一个协程里读写：所有会改变它们的事情（调用方的操作、OkHttp 的回调、定时器到期）
 * 都变成一条 [Command] 排进 [commands]，由 [run] 逐条处理。这样不需要锁，
 * "订阅消息一定在连接成功之后、按注册顺序发出"这类时序也自然成立。
 *
 * 消息分发不走这条队列：它发生在 OkHttp 的读线程上，只读取 [routes] 和 [taps] 这两个不可变快照。
 */
internal class RealReSocket(
    private val config: ReSocketConfig,
    private val transport: Transport,
    private val timeSource: TimeSource.WithComparableMarks,
    dispatcher: CoroutineDispatcher = Dispatchers.Default,
) : ReSocket {
    private val urlProvider = requireNotNull(config.urlProvider)
    private val scope = CoroutineScope(SupervisorJob() + dispatcher)
    private val commands = Channel<Command>(Channel.UNLIMITED)
    private val closed = AtomicBoolean(false)

    private val _state = MutableStateFlow<ConnectionState>(ConnectionState.Disconnected())
    override val state: StateFlow<ConnectionState> = _state.asStateFlow()

    // region 只在 run() 所在的协程里访问

    private var wanted = false
    private var url = ""
    private var attempt = 0
    private var openedAt: ComparableTimeMark? = null
    private var retryJob: Job? = null
    private var heartbeatJob: Job? = null
    private val subscriptions = LinkedHashMap<String, Entry>()

    private class Entry(val subscription: Subscription, val sinks: MutableList<Sink>) {
        /** 在之前的某条连接上发送过，再发就是"恢复"。 */
        var sentBefore = false
    }

    // endregion

    // region 读线程会读

    /** 每次发起连接加一。旧连接迟到的回调带着旧的值，据此丢弃。 */
    @Volatile
    private var generation = 0

    /** 当前这一代的连接，可能还在握手。只在 run() 里写。 */
    @Volatile
    private var connection: Connection? = null

    /** 握手完成、可以发送的连接。只在 run() 里写。 */
    @Volatile
    private var activeConnection: Connection? = null

    @Volatile
    private var lastInboundAt: ComparableTimeMark? = null

    @Volatile
    private var routes: Map<String, List<Sink>> = emptyMap()

    @Volatile
    private var taps: List<Sink> = emptyList()

    // endregion

    init {
        scope.launch { run() }
    }

    override val messages: Flow<String> = collectorFlow(topic = null, backpressure = null) { Command.AddTap(it) }

    override fun subscribe(subscription: Subscription, backpressure: Backpressure?): Flow<String> =
        collectorFlow(subscription.topic, backpressure) { Command.Subscribe(subscription, it) }

    private fun collectorFlow(topic: String?, backpressure: Backpressure?, register: (Sink) -> Command): Flow<String> = flow {
        val collector = currentCoroutineContext()[Job]
        val sink = Sink(topic, backpressure ?: config.backpressure, timeSource, collector) { droppedTopic, count ->
            emit(ReSocketEvent.MessagesDropped(droppedTopic, count))
        }
        // 发不进去说明已经 close 了，直接结束
        if (!commands.trySend(register(sink)).isSuccess) return@flow
        try {
            // 不用 emitAll：它在收集者取消时会自己先 cancel 掉 channel，缓冲区里剩下的消息会被误记成丢弃
            for (message in sink.channel) emit(message)
        } finally {
            sink.detach()
            commands.trySend(Command.RemoveSink(sink))
        }
    }

    override fun connect() {
        commands.trySend(Command.Connect)
    }

    override fun disconnect() {
        commands.trySend(Command.Disconnect)
    }

    override fun reconnectNow() {
        commands.trySend(Command.ReconnectNow)
    }

    override fun send(text: String): Boolean = activeConnection?.send(text) ?: false

    override fun close() {
        if (closed.compareAndSet(false, true)) commands.trySend(Command.Close)
    }

    private sealed interface Command {
        object Connect : Command
        object Disconnect : Command
        object ReconnectNow : Command
        object Close : Command
        class Subscribe(val subscription: Subscription, val sink: Sink) : Command
        class AddTap(val sink: Sink) : Command
        class RemoveSink(val sink: Sink) : Command
        class Opened(val generation: Int) : Command
        class Lost(val generation: Int, val cause: DisconnectCause) : Command
        class RetryDue(val generation: Int) : Command
    }

    private suspend fun run() {
        try {
            processCommands()
        } finally {
            // 正常走到这里是 close()。万一处理命令时出了没预料到的异常，也要把所有 Flow 结束掉，
            // 不能留下一个不再消费命令、收集者永远挂起的客户端；异常本身继续向上抛
            shutDown()
        }
    }

    private suspend fun processCommands() {
        for (command in commands) {
            when (command) {
                Command.Connect -> if (!wanted) {
                    wanted = true
                    attempt = 0
                    startConnecting()
                }
                Command.Disconnect -> if (wanted) {
                    wanted = false
                    dropConnection(graceful = true)
                    _state.value = ConnectionState.Disconnected()
                }
                Command.ReconnectNow -> if (retryJob != null) startConnecting()
                is Command.Subscribe -> addSubscriber(command.subscription, command.sink)
                is Command.AddTap -> taps = taps + command.sink
                is Command.RemoveSink -> removeSink(command.sink)
                is Command.Opened -> if (command.generation == generation) onOpened()
                is Command.Lost -> if (command.generation == generation) onLost(command.cause)
                is Command.RetryDue -> if (command.generation == generation) startConnecting()
                Command.Close -> break
            }
        }
    }

    // region 连接

    private fun startConnecting() {
        dropConnection(graceful = false)
        val current = ++generation
        _state.value = ConnectionState.Connecting(attempt)
        try {
            url = urlProvider()
        } catch (e: Exception) {
            emit(ReSocketEvent.CallbackFailed("url", e))
            onLost(DisconnectCause.Failure(e))
            return
        }
        emit(ReSocketEvent.Connecting(url, attempt))
        try {
            connection = transport.connect(url, ConnectionListener(current))
        } catch (e: Exception) {
            // url 不合法是 IllegalArgumentException；其他异常只可能来自 configureRequest
            if (e !is IllegalArgumentException) emit(ReSocketEvent.CallbackFailed("configureRequest", e))
            onLost(DisconnectCause.Failure(e))
        }
    }

    private fun onOpened() {
        val opened = connection ?: return
        openedAt = timeSource.markNow()
        lastInboundAt = openedAt
        activeConnection = opened
        _state.value = ConnectionState.Connected
        emit(ReSocketEvent.Connected(url))

        val greeting = try {
            config.greeting?.invoke().orEmpty()
        } catch (e: Exception) {
            emit(ReSocketEvent.CallbackFailed("greeting", e))
            emptyList()
        }
        greeting.forEach { sendOrReport(opened, it) }
        for (entry in subscriptions.values) {
            if (sendOrReport(opened, entry.subscription.subscribeMessage)) {
                emit(ReSocketEvent.Subscribed(entry.subscription.topic, restored = entry.sentBefore))
                entry.sentBefore = true
            }
        }
        startHeartbeat(opened)
    }

    private fun onLost(cause: DisconnectCause) {
        val wasStable = openedAt?.let { it.elapsedNow() >= config.stableAfter } == true
        dropConnection(graceful = false)
        emit(ReSocketEvent.Disconnected(url, cause))
        if (!wanted) return

        if (wasStable) attempt = 0
        attempt++
        val delay = try {
            config.reconnectPolicy.nextDelay(attempt, cause)
        } catch (e: Exception) {
            emit(ReSocketEvent.CallbackFailed("reconnectPolicy", e))
            null
        }
        if (delay == null) {
            wanted = false
            _state.value = ConnectionState.Disconnected(cause)
            emit(ReSocketEvent.ReconnectAbandoned(cause))
            return
        }
        _state.value = ConnectionState.WaitingToReconnect(attempt, delay, cause)
        emit(ReSocketEvent.ReconnectScheduled(attempt, delay))
        val current = generation
        retryJob = scope.launch {
            delay(delay)
            commands.trySend(Command.RetryDue(current))
        }
    }

    /** 放下当前连接和所有定时器。generation 加一，之后这条连接的任何回调都会被忽略。 */
    private fun dropConnection(graceful: Boolean) {
        generation++
        retryJob?.cancel()
        retryJob = null
        heartbeatJob?.cancel()
        heartbeatJob = null
        // 还没握手完成的连接没法走关闭握手，只能直接取消
        val opened = activeConnection != null
        activeConnection = null
        openedAt = null
        connection?.let { if (graceful && opened) it.close() else it.cancel() }
        connection = null
    }

    private fun startHeartbeat(target: Connection) {
        val heartbeat = config.heartbeat as? Heartbeat.Text ?: return
        val current = generation
        heartbeatJob = scope.launch {
            while (true) {
                delay(heartbeat.interval)
                val sentAt = timeSource.markNow()
                val sent = try {
                    target.send(heartbeat.message())
                } catch (e: Exception) {
                    emit(ReSocketEvent.CallbackFailed("heartbeat", e))
                    false
                }
                if (sent) delay(heartbeat.timeout)
                val last = lastInboundAt
                if (!sent || last == null || last < sentAt) {
                    val error = HeartbeatTimeoutException(
                        if (sent) "发出心跳后 ${heartbeat.timeout} 内没有收到任何数据" else "心跳发送失败",
                    )
                    commands.trySend(Command.Lost(current, DisconnectCause.Failure(error)))
                    return@launch
                }
            }
        }
    }

    private inner class ConnectionListener(private val generation: Int) : TransportListener {
        override fun onOpen() {
            commands.trySend(Command.Opened(generation))
        }

        override fun onText(text: String) = dispatch(generation, text)

        override fun onBinary(bytes: ByteString) {
            val decoder = config.binaryDecoder ?: return
            val text = guard("binaryDecoder") { decoder(bytes) } ?: return
            dispatch(generation, text)
        }

        override fun onClosed(code: Int, reason: String) {
            commands.trySend(Command.Lost(generation, DisconnectCause.Closed(code, reason)))
        }

        override fun onFailure(error: Throwable) {
            commands.trySend(Command.Lost(generation, DisconnectCause.Failure(error)))
        }
    }

    // endregion

    // region 分发（OkHttp 读线程）

    private fun dispatch(from: Int, message: String) {
        if (from != generation) return
        lastInboundAt = timeSource.markNow()

        config.autoReply?.let { autoReply ->
            // 用 connection 而不是 activeConnection：服务端可能在 onOpen 被处理之前就发来第一条消息
            guard("autoReply") { autoReply(message) }?.let { connection?.send(it) }
        }
        for (tap in taps) tap.offer(message)

        val currentRoutes = routes
        if (currentRoutes.isEmpty()) return
        val topic = config.topicOf?.let { topicOf -> guard("topicOf") { topicOf(message) } } ?: return
        currentRoutes[topic]?.forEach { it.offer(message) }
    }

    private inline fun <T> guard(name: String, block: () -> T?): T? =
        try {
            block()
        } catch (e: Exception) {
            emit(ReSocketEvent.CallbackFailed(name, e))
            null
        }

    // endregion

    // region 订阅

    private fun addSubscriber(subscription: Subscription, sink: Sink) {
        val existing = subscriptions[subscription.topic]
        if (existing != null) {
            existing.sinks += sink
        } else {
            val entry = Entry(subscription, arrayListOf(sink))
            subscriptions[subscription.topic] = entry
            // 没连上时只登记，连上之后 onOpened 会统一发送
            val target = activeConnection
            if (target != null && sendOrReport(target, subscription.subscribeMessage)) {
                entry.sentBefore = true
                emit(ReSocketEvent.Subscribed(subscription.topic, restored = false))
            }
        }
        publishRoutes()
    }

    private fun removeSink(sink: Sink) {
        val topic = sink.topic
        if (topic == null) {
            taps = taps - sink
            return
        }
        val entry = subscriptions[topic] ?: return
        if (!entry.sinks.remove(sink)) return
        if (entry.sinks.isEmpty()) {
            subscriptions.remove(topic)
            val target = activeConnection
            val message = entry.subscription.unsubscribeMessage
            if (target != null && message != null && sendOrReport(target, message)) {
                emit(ReSocketEvent.Unsubscribed(topic))
            }
        }
        publishRoutes()
    }

    private fun publishRoutes() {
        routes = subscriptions.entries.associate { (topic, entry) -> topic to entry.sinks.toList() }
    }

    private fun sendOrReport(target: Connection, message: String): Boolean {
        val sent = target.send(message)
        if (!sent) emit(ReSocketEvent.SendFailed(message))
        return sent
    }

    // endregion

    private fun shutDown() {
        wanted = false
        dropConnection(graceful = true)
        _state.value = ConnectionState.Closed

        // 关掉队列之后再收尾：此刻之前排进来的订阅要结束掉，否则它们的 Flow 会永远挂着
        commands.close()
        while (true) {
            when (val pending = commands.tryReceive().getOrNull() ?: break) {
                is Command.Subscribe -> pending.sink.complete()
                is Command.AddTap -> pending.sink.complete()
                else -> Unit
            }
        }
        subscriptions.values.forEach { entry -> entry.sinks.forEach(Sink::complete) }
        taps.forEach(Sink::complete)
        subscriptions.clear()
        routes = emptyMap()
        taps = emptyList()
        scope.cancel()
    }

    private fun emit(event: ReSocketEvent) {
        val listener = config.listener ?: return
        try {
            listener.onEvent(event)
        } catch (_: Exception) {
            // 见 ReSocketListener 的说明
        }
    }
}
