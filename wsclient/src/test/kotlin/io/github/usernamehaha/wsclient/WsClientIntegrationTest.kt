package io.github.usernamehaha.wsclient

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.ByteString.Companion.encodeUtf8
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

/** 用真实的 OkHttp 和本地 WebSocket 服务端走一遍完整链路。状态机的细节在 RealWsClientTest 里用虚拟时间覆盖。 */
class WsClientIntegrationTest {
    private val server = MockWebServer()
    private val okHttpClient = OkHttpClient()
    private val scope = CoroutineScope(Dispatchers.Default)

    /** 服务端视角的一条连接。 */
    private class ServerSocket : WebSocketListener() {
        val opened = LinkedBlockingQueue<WebSocket>()
        val received = LinkedBlockingQueue<String>()

        override fun onOpen(webSocket: WebSocket, response: Response) {
            opened += webSocket
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
            received += text
        }

        override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
            webSocket.close(code, reason)
        }

        fun awaitOpen(): WebSocket = checkNotNull(opened.poll(5, TimeUnit.SECONDS)) { "客户端没有连上来" }
        fun awaitMessage(): String = checkNotNull(received.poll(5, TimeUnit.SECONDS)) { "没有收到客户端的消息" }
    }

    private fun enqueueSocket(): ServerSocket =
        ServerSocket().also { server.enqueue(MockResponse().withWebSocketUpgrade(it)) }

    @After
    fun tearDown() {
        scope.cancel()
        server.shutdown()
        okHttpClient.dispatcher.executorService.shutdown()
    }

    @Test
    fun `订阅、收消息、服务端断开后重连并恢复订阅`(): Unit = runBlocking {
        val first = enqueueSocket()
        val second = enqueueSocket()
        val client = WsClient.create(okHttpClient) {
            url(server.url("/ws").toString().replaceFirst("http", "ws"))
            heartbeat = Heartbeat.none()
            reconnectPolicy = ReconnectPolicy.exponentialBackoff(initialDelay = 50.milliseconds, jitter = 0.0)
            topicOf = { it.substringBefore(':') }
            configureRequest = { it.header("X-Token", "secret") }
        }
        val trades = Channel<String>(Channel.UNLIMITED)
        scope.launch { client.subscribe(Subscription("trade", "sub:trade", "unsub:trade")).collect(trades::send) }

        client.connect()
        val firstSocket = first.awaitOpen()
        assertEquals("secret", server.takeRequest().getHeader("X-Token"))
        assertEquals("sub:trade", first.awaitMessage())

        firstSocket.send("trade:1")
        firstSocket.send("depth:ignored")
        assertEquals("trade:1", withTimeout(5.seconds) { trades.receive() })

        firstSocket.close(1001, "restarting")
        val secondSocket = second.awaitOpen()
        assertEquals("sub:trade", second.awaitMessage())
        secondSocket.send("trade:2")
        assertEquals("trade:2", withTimeout(5.seconds) { trades.receive() })

        assertTrue(client.send("hello"))
        assertEquals("hello", second.awaitMessage())

        client.close()
        withTimeout(5.seconds) { client.state.first { it == ConnectionState.Closed } }
    }

    @Test
    fun `二进制帧经过解码器后分发`(): Unit = runBlocking {
        val socket = enqueueSocket()
        val client = WsClient.create(okHttpClient) {
            url(server.url("/ws").toString().replaceFirst("http", "ws"))
            binaryDecoder = { it.utf8() }
        }
        val messages = Channel<String>(Channel.UNLIMITED)
        scope.launch { client.messages.collect(messages::send) }
        client.connect()
        socket.awaitOpen().send("binary payload".encodeUtf8())
        assertEquals("binary payload", withTimeout(5.seconds) { messages.receive() })
        client.close()
    }

    @Test
    fun `服务端拒绝升级时进入重连`(): Unit = runBlocking {
        server.enqueue(MockResponse().setResponseCode(503))
        val client = WsClient.create(okHttpClient) {
            url(server.url("/ws").toString().replaceFirst("http", "ws"))
            reconnectPolicy = ReconnectPolicy.exponentialBackoff(initialDelay = 30.seconds, jitter = 0.0)
        }
        client.connect()
        val waiting = withTimeout(5.seconds) { client.state.first { it is ConnectionState.WaitingToReconnect } }
        assertTrue((waiting as ConnectionState.WaitingToReconnect).cause is DisconnectCause.Failure)
        client.close()
    }

    @Test
    fun `configureRequest 抛异常时进入重连，而不是让客户端停摆`(): Unit = runBlocking {
        val client = WsClient.create(okHttpClient) {
            url(server.url("/ws").toString().replaceFirst("http", "ws"))
            configureRequest = { error("token not ready") }
        }
        client.connect()
        withTimeout(5.seconds) { client.state.first { it is ConnectionState.WaitingToReconnect } }
        client.close()
        withTimeout(5.seconds) { client.state.first { it == ConnectionState.Closed } }
    }

    @Test
    fun `没有配置 url 时创建失败`() {
        assertThrows(IllegalArgumentException::class.java) { WsClient.create(okHttpClient) { } }
    }
}
