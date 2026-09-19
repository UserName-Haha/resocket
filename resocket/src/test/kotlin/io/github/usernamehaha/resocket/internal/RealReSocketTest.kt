package io.github.usernamehaha.resocket.internal

import io.github.usernamehaha.resocket.Backpressure
import io.github.usernamehaha.resocket.ConnectionState
import io.github.usernamehaha.resocket.DisconnectCause
import io.github.usernamehaha.resocket.Heartbeat
import io.github.usernamehaha.resocket.HeartbeatTimeoutException
import io.github.usernamehaha.resocket.ReconnectPolicy
import io.github.usernamehaha.resocket.Subscription
import io.github.usernamehaha.resocket.ReSocketConfig
import io.github.usernamehaha.resocket.ReSocketEvent
import java.io.IOException
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import okio.ByteString.Companion.encodeUtf8
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class RealReSocketTest {
    private val transport = FakeTransport()
    private val events = CopyOnWriteArrayList<ReSocketEvent>()

    private val trades = Subscription("trade", """{"op":"sub","topic":"trade"}""", """{"op":"unsub","topic":"trade"}""")
    private val depth = Subscription("depth", """{"op":"sub","topic":"depth"}""", """{"op":"unsub","topic":"depth"}""")

    private fun TestScope.client(configure: ReSocketConfig.() -> Unit = {}): RealReSocket {
        val config = ReSocketConfig().apply {
            url("wss://example.com/ws")
            heartbeat = Heartbeat.none()
            reconnectPolicy = ReconnectPolicy.exponentialBackoff(jitter = 0.0)
            // 测试消息的格式是 "topic:内容"
            topicOf = { it.substringBefore(':', "").ifEmpty { null } }
            listener { events += it }
            configure()
        }
        return RealReSocket(config, transport, testScheduler.timeSource, StandardTestDispatcher(testScheduler))
    }

    /** 在后台收集并返回已收到的消息列表。 */
    private fun TestScope.collect(flow: Flow<String>): Pair<List<String>, Job> {
        val received = CopyOnWriteArrayList<String>()
        val job = launch { flow.collect { received += it } }
        runCurrent()
        return received to job
    }

    private fun TestScope.connected(client: RealReSocket): FakeConnection {
        client.connect()
        runCurrent()
        transport.latest.open()
        runCurrent()
        return transport.latest
    }

    private inline fun <reified T : ReSocketEvent> events(): List<T> = events.filterIsInstance<T>()

    // region 状态机

    @Test
    fun `连接成功的状态流转`() = runTest {
        val client = client()
        assertEquals(ConnectionState.Disconnected(), client.state.value)

        client.connect()
        runCurrent()
        assertEquals(ConnectionState.Connecting(0), client.state.value)
        assertEquals("wss://example.com/ws", transport.latest.url)

        transport.latest.open()
        runCurrent()
        assertSame(ConnectionState.Connected, client.state.value)
        client.close()
    }

    @Test
    fun `重复 connect 不会建立多条连接`() = runTest {
        val client = client()
        repeat(3) { client.connect() }
        runCurrent()
        transport.latest.open()
        client.connect()
        runCurrent()
        assertEquals(1, transport.connections.size)
        client.close()
    }

    @Test
    fun `连接失败后按退避延迟重连`() = runTest {
        val client = client()
        client.connect()
        runCurrent()
        val error = IOException("refused")
        transport.latest.fail(error)
        runCurrent()
        assertEquals(ConnectionState.WaitingToReconnect(1, 1.seconds, DisconnectCause.Failure(error)), client.state.value)

        advanceTimeBy(999.milliseconds)
        assertEquals(1, transport.connections.size)
        advanceTimeBy(2.milliseconds)
        assertEquals(ConnectionState.Connecting(1), client.state.value)
        assertEquals(2, transport.connections.size)

        transport.latest.fail(IOException())
        runCurrent()
        assertEquals(2.seconds, (client.state.value as ConnectionState.WaitingToReconnect).delay)
        advanceTimeBy(2.seconds + 1.milliseconds)
        transport.latest.fail(IOException())
        runCurrent()
        assertEquals(4.seconds, (client.state.value as ConnectionState.WaitingToReconnect).delay)
        assertEquals(listOf(1, 2, 3), events<ReSocketEvent.ReconnectScheduled>().map { it.attempt })
        client.close()
    }

    @Test
    fun `连接稳定之后断开，重连计数从头开始`() = runTest {
        val client = client()
        client.connect()
        runCurrent()
        transport.latest.fail(IOException())
        advanceTimeBy(1.seconds + 1.milliseconds)
        transport.latest.open()
        advanceTimeBy(10.seconds)

        transport.latest.fail(IOException())
        runCurrent()
        assertEquals(1, (client.state.value as ConnectionState.WaitingToReconnect).attempt)
        client.close()
    }

    @Test
    fun `连上后马上被踢，退避继续增长`() = runTest {
        val client = client()
        val connection = connected(client)
        connection.serverClose(4001, "auth failed")
        runCurrent()
        assertEquals(1.seconds, (client.state.value as ConnectionState.WaitingToReconnect).delay)

        advanceTimeBy(1.seconds + 1.milliseconds)
        transport.latest.open()
        advanceTimeBy(5.seconds)
        transport.latest.serverClose(4001, "auth failed")
        runCurrent()
        val waiting = client.state.value as ConnectionState.WaitingToReconnect
        assertEquals(2, waiting.attempt)
        assertEquals(2.seconds, waiting.delay)
        assertEquals(DisconnectCause.Closed(4001, "auth failed"), waiting.cause)
        client.close()
    }

    @Test
    fun `策略放弃后回到 Disconnected 并带上原因，之后可以再次 connect`() = runTest {
        val client = client { reconnectPolicy = ReconnectPolicy.exponentialBackoff(jitter = 0.0, maxAttempts = 1) }
        client.connect()
        runCurrent()
        transport.latest.fail(IOException())
        advanceTimeBy(1.seconds + 1.milliseconds)
        val error = IOException("still down")
        transport.latest.fail(error)
        runCurrent()

        assertEquals(ConnectionState.Disconnected(DisconnectCause.Failure(error)), client.state.value)
        assertEquals(1, events<ReSocketEvent.ReconnectAbandoned>().size)
        advanceTimeBy(60.seconds)
        assertEquals(2, transport.connections.size)

        client.connect()
        runCurrent()
        assertEquals(ConnectionState.Connecting(0), client.state.value)
        client.close()
    }

    @Test
    fun `策略抛异常时放弃重连并发事件`() = runTest {
        val client = client { reconnectPolicy = ReconnectPolicy { _, _ -> error("bad policy") } }
        client.connect()
        runCurrent()
        transport.latest.fail(IOException())
        runCurrent()
        assertTrue(client.state.value is ConnectionState.Disconnected)
        assertEquals("reconnectPolicy", events<ReSocketEvent.CallbackFailed>().single().name)
        client.close()
    }

    @Test
    fun `disconnect 走关闭握手并停止重连`() = runTest {
        val client = client()
        val connection = connected(client)
        client.disconnect()
        runCurrent()
        assertTrue(connection.closed)
        assertEquals(ConnectionState.Disconnected(), client.state.value)

        // 关闭握手完成后的回调属于旧连接，不能触发重连
        connection.serverClose(1000)
        advanceTimeBy(60.seconds)
        assertEquals(1, transport.connections.size)
        assertEquals(ConnectionState.Disconnected(), client.state.value)
        client.close()
    }

    @Test
    fun `握手还没完成时 disconnect 直接取消连接`() = runTest {
        val client = client()
        client.connect()
        runCurrent()
        client.disconnect()
        runCurrent()
        assertTrue(transport.latest.cancelled)
        assertFalse(transport.latest.closed)
        client.close()
    }

    @Test
    fun `等待重连期间 disconnect 会取消定时器`() = runTest {
        val client = client()
        client.connect()
        runCurrent()
        transport.latest.fail(IOException())
        runCurrent()
        client.disconnect()
        advanceTimeBy(60.seconds)
        assertEquals(1, transport.connections.size)
        assertEquals(ConnectionState.Disconnected(), client.state.value)
        client.close()
    }

    @Test
    fun `reconnectNow 跳过剩余等待，其他状态下不起作用`() = runTest {
        val client = client()
        client.reconnectNow()
        val connection = connected(client)
        client.reconnectNow()
        runCurrent()
        assertEquals(1, transport.connections.size)

        connection.fail(IOException())
        runCurrent()
        client.reconnectNow()
        runCurrent()
        assertEquals(ConnectionState.Connecting(1), client.state.value)
        assertEquals(2, transport.connections.size)

        // 原来的定时器到期不能再发起一次
        advanceTimeBy(5.seconds)
        assertEquals(2, transport.connections.size)
        client.close()
    }

    @Test
    fun `旧连接迟到的回调被忽略`() = runTest {
        val client = client()
        val (received, job) = collect(client.messages)
        val old = connected(client)
        old.fail(IOException())
        advanceTimeBy(1.seconds + 1.milliseconds)
        transport.latest.open()
        runCurrent()

        old.fail(IOException("late"))
        old.open()
        old.receive("trade:stale")
        runCurrent()
        assertSame(ConnectionState.Connected, client.state.value)
        assertTrue(received.isEmpty())
        job.cancelAndJoin()
        client.close()
    }

    @Test
    fun `每次连接都重新取 url，取 url 失败按连接失败处理`() = runTest {
        var calls = 0
        val client = client {
            url {
                calls++
                when (calls) {
                    1 -> "wss://a.example.com/ws"
                    2 -> error("no host available")
                    3 -> "https://not-a-websocket"
                    else -> "wss://b.example.com/ws"
                }
            }
        }
        client.connect()
        runCurrent()
        transport.latest.fail(IOException())
        advanceTimeBy(1.seconds + 1.milliseconds)
        assertEquals("url", events<ReSocketEvent.CallbackFailed>().single().name)
        assertEquals(2, (client.state.value as ConnectionState.WaitingToReconnect).attempt)

        advanceTimeBy(2.seconds + 1.milliseconds)
        assertEquals(3, (client.state.value as ConnectionState.WaitingToReconnect).attempt)

        advanceTimeBy(4.seconds + 1.milliseconds)
        assertEquals("wss://b.example.com/ws", transport.latest.url)
        client.close()
    }

    @Test
    fun `建立连接时抛出的任何异常都按连接失败处理，不会让客户端停摆`() = runTest {
        val client = client()
        transport.connectError = IllegalStateException("token not ready")
        client.connect()
        runCurrent()
        assertEquals(1, (client.state.value as ConnectionState.WaitingToReconnect).attempt)
        assertEquals("configureRequest", events<ReSocketEvent.CallbackFailed>().single().name)

        transport.connectError = null
        advanceTimeBy(1.seconds + 1.milliseconds)
        transport.latest.open()
        runCurrent()
        assertSame(ConnectionState.Connected, client.state.value)
        client.close()
    }

    // endregion

    // region 订阅

    @Test
    fun `连接之前的订阅在连上后发送，连接之后的订阅立即发送`() = runTest {
        val client = client()
        val (_, first) = collect(client.subscribe(trades))
        val connection = connected(client)
        assertEquals(listOf(trades.subscribeMessage), connection.sent)

        val (_, second) = collect(client.subscribe(depth))
        assertEquals(listOf(trades.subscribeMessage, depth.subscribeMessage), connection.sent)
        assertEquals(listOf(false, false), events<ReSocketEvent.Subscribed>().map { it.restored })
        first.cancelAndJoin()
        second.cancelAndJoin()
        client.close()
    }

    @Test
    fun `同一个 topic 多个收集者只订阅一次，最后一个离开时才退订`() = runTest {
        val client = client()
        val connection = connected(client)
        val (firstReceived, first) = collect(client.subscribe(trades))
        val (secondReceived, second) = collect(client.subscribe(trades))
        assertEquals(1, connection.sent.size)

        connection.receive("trade:1")
        runCurrent()
        assertEquals(listOf("trade:1"), firstReceived)
        assertEquals(listOf("trade:1"), secondReceived)

        first.cancelAndJoin()
        runCurrent()
        assertEquals(1, connection.sent.size)

        second.cancelAndJoin()
        runCurrent()
        assertEquals(listOf(trades.subscribeMessage, trades.unsubscribeMessage), connection.sent)
        assertEquals("trade", events<ReSocketEvent.Unsubscribed>().single().topic)
        client.close()
    }

    @Test
    fun `重连后先发 greeting，再按注册顺序恢复仍然有效的订阅`() = runTest {
        var logins = 0
        val client = client { greeting = { listOf("login:${++logins}") } }
        val first = connected(client)
        val (received, tradesJob) = collect(client.subscribe(trades))
        val (_, depthJob) = collect(client.subscribe(depth))
        val (_, cancelled) = collect(client.subscribe(Subscription("kline", "sub-kline")))
        cancelled.cancelAndJoin()
        runCurrent()

        first.fail(IOException())
        advanceTimeBy(1.seconds + 1.milliseconds)
        val second = transport.latest
        assertTrue(second.sent.isEmpty())
        second.open()
        runCurrent()

        assertEquals(listOf("login:2", trades.subscribeMessage, depth.subscribeMessage), second.sent)
        assertEquals(
            listOf("trade" to true, "depth" to true),
            events<ReSocketEvent.Subscribed>().takeLast(2).map { it.topic to it.restored },
        )
        second.receive("trade:after-reconnect")
        runCurrent()
        assertEquals(listOf("trade:after-reconnect"), received)
        tradesJob.cancelAndJoin()
        depthJob.cancelAndJoin()
        client.close()
    }

    @Test
    fun `断线期间收集者不结束，断线期间退订的不会被恢复`() = runTest {
        val client = client()
        val first = connected(client)
        val (_, tradesJob) = collect(client.subscribe(trades))
        val (_, depthJob) = collect(client.subscribe(depth))
        first.fail(IOException())
        runCurrent()
        assertTrue(tradesJob.isActive)

        depthJob.cancelAndJoin()
        advanceTimeBy(1.seconds + 1.milliseconds)
        transport.latest.open()
        runCurrent()
        assertEquals(listOf(trades.subscribeMessage), transport.latest.sent)
        tradesJob.cancelAndJoin()
        client.close()
    }

    @Test
    fun `没有退订消息的订阅离开时什么都不发`() = runTest {
        val client = client()
        val connection = connected(client)
        val (_, job) = collect(client.subscribe(Subscription("kline", "sub-kline")))
        job.cancelAndJoin()
        runCurrent()
        assertEquals(listOf("sub-kline"), connection.sent)
        assertTrue(events<ReSocketEvent.Unsubscribed>().isEmpty())
        client.close()
    }

    @Test
    fun `订阅消息发送失败时发事件，下次重连后恢复`() = runTest {
        val client = client()
        val connection = connected(client)
        connection.sendSucceeds = false
        val (_, job) = collect(client.subscribe(trades))
        assertEquals(trades.subscribeMessage, events<ReSocketEvent.SendFailed>().single().message)

        connection.fail(IOException())
        advanceTimeBy(1.seconds + 1.milliseconds)
        transport.latest.open()
        runCurrent()
        assertEquals(listOf(trades.subscribeMessage), transport.latest.sent)
        assertEquals(listOf(false), events<ReSocketEvent.Subscribed>().map { it.restored })
        job.cancelAndJoin()
        client.close()
    }

    // endregion

    // region 分发与背压

    @Test
    fun `消息按 topic 分发，messages 收到全部`() = runTest {
        val client = client()
        val connection = connected(client)
        val (all, allJob) = collect(client.messages)
        val (tradeMessages, tradesJob) = collect(client.subscribe(trades))
        val (depthMessages, depthJob) = collect(client.subscribe(depth))

        listOf("trade:1", "depth:1", "pong", "kline:1", "trade:2").forEach(connection::receive)
        runCurrent()
        assertEquals(listOf("trade:1", "trade:2"), tradeMessages)
        assertEquals(listOf("depth:1"), depthMessages)
        assertEquals(listOf("trade:1", "depth:1", "pong", "kline:1", "trade:2"), all)
        listOf(allJob, tradesJob, depthJob).forEach { it.cancelAndJoin() }
        client.close()
    }

    @Test
    fun `topicOf 抛异常时消息仍然出现在 messages 里`() = runTest {
        val client = client { topicOf = { error("bad json") } }
        val connection = connected(client)
        val (all, allJob) = collect(client.messages)
        val (tradeMessages, tradesJob) = collect(client.subscribe(trades))
        connection.receive("trade:1")
        runCurrent()
        assertEquals(listOf("trade:1"), all)
        assertTrue(tradeMessages.isEmpty())
        assertEquals("topicOf", events<ReSocketEvent.CallbackFailed>().single().name)
        allJob.cancelAndJoin()
        tradesJob.cancelAndJoin()
        client.close()
    }

    @Test
    fun `dropOldest 保留最新的消息并上报丢弃数`() = runTest {
        val client = client()
        val connection = connected(client)
        val (received, job) = collect(client.subscribe(trades, Backpressure.dropOldest(3)))
        // 第一条直接交给了正在等待的收集者，之后它没有机会运行，其余 9 条都压在容量为 3 的缓冲区上
        repeat(10) { connection.receive("trade:$it") }
        runCurrent()
        assertEquals(listOf("trade:0", "trade:7", "trade:8", "trade:9"), received)
        assertEquals(listOf(1L), events<ReSocketEvent.MessagesDropped>().map { it.count })

        // 一秒内的其余丢弃攒到下一次上报
        advanceTimeBy(1.seconds + 1.milliseconds)
        repeat(5) { connection.receive("trade:late$it") }
        runCurrent()
        val dropped = events<ReSocketEvent.MessagesDropped>()
        assertTrue(dropped.all { it.topic == "trade" })
        assertEquals(listOf(1L, 6L), dropped.map { it.count })
        job.cancelAndJoin()
        client.close()
    }

    @Test
    fun `dropNewest 保留最早的消息`() = runTest {
        val client = client()
        val connection = connected(client)
        val (received, job) = collect(client.subscribe(trades, Backpressure.dropNewest(3)))
        repeat(10) { connection.receive("trade:$it") }
        runCurrent()
        assertEquals(listOf("trade:0", "trade:1", "trade:2", "trade:3"), received)
        job.cancelAndJoin()
        client.close()
    }

    @Test
    fun `latest 只保留最新一条，unbounded 一条不丢`() = runTest {
        val client = client()
        val connection = connected(client)
        val (latest, latestJob) = collect(client.subscribe(trades, Backpressure.latest()))
        val (everything, everythingJob) = collect(client.subscribe(trades, Backpressure.unbounded()))
        repeat(1000) { connection.receive("trade:$it") }
        runCurrent()
        assertEquals(listOf("trade:0", "trade:999"), latest)
        assertEquals(1000, everything.size)
        latestJob.cancelAndJoin()
        everythingJob.cancelAndJoin()
        client.close()
    }

    @Test
    fun `慢收集者不影响其他收集者`() = runTest {
        val client = client()
        val connection = connected(client)
        val slow = launch { client.subscribe(trades, Backpressure.dropOldest(1)).collect { kotlinx.coroutines.delay(1.seconds) } }
        val (fast, fastJob) = collect(client.subscribe(trades))
        repeat(5) {
            connection.receive("trade:$it")
            runCurrent()
        }
        assertEquals(5, fast.size)
        slow.cancelAndJoin()
        fastJob.cancelAndJoin()
        client.close()
    }

    @Test
    fun `收集者离开时缓冲区里剩下的消息不算丢弃`() = runTest {
        val client = client()
        val connection = connected(client)
        val (_, job) = collect(client.subscribe(trades))
        repeat(5) { connection.receive("trade:$it") }
        job.cancelAndJoin()
        runCurrent()
        assertTrue(events<ReSocketEvent.MessagesDropped>().isEmpty())
        client.close()
    }

    @Test
    fun `二进制帧经过解码后和文本消息一样分发，没有解码器时忽略`() = runTest {
        val plain = client()
        val (ignored, ignoredJob) = collect(plain.messages)
        connected(plain).receive("trade:binary".encodeUtf8())
        runCurrent()
        assertTrue(ignored.isEmpty())
        ignoredJob.cancelAndJoin()
        plain.close()

        val decoding = client { binaryDecoder = { it.utf8() } }
        val (received, job) = collect(decoding.subscribe(trades))
        connected(decoding).receive("trade:binary".encodeUtf8())
        runCurrent()
        assertEquals(listOf("trade:binary"), received)
        job.cancelAndJoin()
        decoding.close()
    }

    @Test
    fun `autoReply 应答服务端的心跳`() = runTest {
        val client = client { autoReply = { if (it.startsWith("ping:")) "pong:" + it.substringAfter(':') else null } }
        val connection = connected(client)
        connection.receive("ping:42")
        connection.receive("trade:1")
        assertEquals(listOf("pong:42"), connection.sent)
        client.close()
    }

    // endregion

    // region 心跳

    @Test
    fun `按间隔发送心跳，有数据进来就认为连接活着`() = runTest {
        var sequence = 0
        val client = client { heartbeat = Heartbeat.text(15.seconds, 10.seconds) { "ping:${++sequence}" } }
        val connection = connected(client)

        advanceTimeBy(15.seconds + 1.milliseconds)
        assertEquals(listOf("ping:1"), connection.sent)
        connection.receive("trade:1")
        advanceTimeBy(10.seconds)
        assertSame(ConnectionState.Connected, client.state.value)

        advanceTimeBy(15.seconds)
        assertEquals(listOf("ping:1", "ping:2"), connection.sent)
        connection.receive("pong")
        advanceTimeBy(10.seconds)
        assertSame(ConnectionState.Connected, client.state.value)
        client.close()
    }

    @Test
    fun `心跳超时后断开并重连`() = runTest {
        val client = client { heartbeat = Heartbeat.text(15.seconds, 10.seconds) { "ping" } }
        val connection = connected(client)
        // 心跳发出之前收到的数据不能证明之后连接还活着
        advanceTimeBy(14.seconds)
        connection.receive("trade:1")
        advanceTimeBy(11.seconds + 2.milliseconds)

        val waiting = client.state.value as ConnectionState.WaitingToReconnect
        assertTrue((waiting.cause as DisconnectCause.Failure).error is HeartbeatTimeoutException)
        assertTrue(connection.cancelled)

        advanceTimeBy(1.seconds + 1.milliseconds)
        assertEquals(2, transport.connections.size)
        client.close()
    }

    @Test
    fun `心跳发不出去视为连接失效`() = runTest {
        val client = client { heartbeat = Heartbeat.text(15.seconds, 10.seconds) { "ping" } }
        val connection = connected(client)
        connection.sendSucceeds = false
        advanceTimeBy(15.seconds + 2.milliseconds)
        assertTrue(client.state.value is ConnectionState.WaitingToReconnect)
        client.close()
    }

    @Test
    fun `断开后心跳停止`() = runTest {
        val client = client { heartbeat = Heartbeat.text(15.seconds, 10.seconds) { "ping" } }
        val connection = connected(client)
        client.disconnect()
        advanceTimeBy(60.seconds)
        assertTrue(connection.sent.isEmpty())
        client.close()
    }

    // endregion

    // region send 与 close

    @Test
    fun `未连接时 send 返回 false，不会留到重连后补发`() = runTest {
        val client = client()
        assertFalse(client.send("order"))
        client.connect()
        runCurrent()
        assertFalse(client.send("order"))

        transport.latest.open()
        runCurrent()
        assertTrue(client.send("order"))
        assertEquals(listOf("order"), transport.latest.sent)

        transport.latest.fail(IOException())
        runCurrent()
        assertFalse(client.send("order"))
        client.close()
    }

    @Test
    fun `close 之后收集者正常结束，状态为 Closed，之后的操作不起作用`() = runTest {
        val client = client()
        val connection = connected(client)
        val collected = async { client.subscribe(trades).toList() }
        val tapped = async { client.messages.toList() }
        runCurrent()
        connection.receive("trade:1")

        client.close()
        client.close()
        runCurrent()
        assertEquals(listOf("trade:1"), collected.await())
        assertEquals(listOf("trade:1"), tapped.await())
        assertSame(ConnectionState.Closed, client.state.value)
        assertTrue(connection.closed)

        client.connect()
        advanceTimeBy(60.seconds)
        assertEquals(1, transport.connections.size)
        assertFalse(client.send("x"))
        assertEquals(emptyList<String>(), client.subscribe(trades).toList())
    }

    @Test
    fun `监听器抛异常不影响连接`() = runTest {
        val client = client { listener { error("listener bug") } }
        connected(client)
        assertSame(ConnectionState.Connected, client.state.value)
        client.close()
    }

    // endregion
}
