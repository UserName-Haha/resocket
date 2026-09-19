package io.github.usernamehaha.resocket.internal

import io.github.usernamehaha.resocket.Heartbeat
import io.github.usernamehaha.resocket.ReSocketConfig
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString

/** 把 OkHttp 隔在这层接口后面，状态机的测试不需要真实的网络和时间。 */
internal interface Transport {
    /** @throws IllegalArgumentException [url] 不合法 */
    fun connect(url: String, listener: TransportListener): Connection
}

internal interface Connection {
    fun send(text: String): Boolean
    fun close()
    fun cancel()
}

/** [onClosed] 和 [onFailure] 二者至多回调一次，之后不再有任何回调。 */
internal interface TransportListener {
    fun onOpen()
    fun onText(text: String)
    fun onBinary(bytes: ByteString)
    fun onClosed(code: Int, reason: String)
    fun onFailure(error: Throwable)
}

internal class OkHttpTransport(client: OkHttpClient, private val config: ReSocketConfig) : Transport {
    private val client: OkHttpClient = when (val heartbeat = config.heartbeat) {
        is Heartbeat.ProtocolPing ->
            client.newBuilder().pingInterval(heartbeat.interval.inWholeMilliseconds, TimeUnit.MILLISECONDS).build()
        // 调用方的 client 上可能配了 pingInterval，这里要以库的配置为准
        else -> client.newBuilder().pingInterval(0, TimeUnit.MILLISECONDS).build()
    }

    override fun connect(url: String, listener: TransportListener): Connection {
        val request = Request.Builder().url(url).also { config.configureRequest?.invoke(it) }.build()
        val finished = AtomicBoolean(false)
        val webSocket = client.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) = listener.onOpen()

            override fun onMessage(webSocket: WebSocket, text: String) = listener.onText(text)

            override fun onMessage(webSocket: WebSocket, bytes: ByteString) = listener.onBinary(bytes)

            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                // 服务端发起关闭。回一个 close 帧完成握手；对上层来说连接此刻已经结束，
                // 不等 onClosed —— 对端不配合的话它要 60 秒后才来
                webSocket.close(NORMAL_CLOSURE, null)
                if (finished.compareAndSet(false, true)) listener.onClosed(code, reason)
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                if (finished.compareAndSet(false, true)) listener.onClosed(code, reason)
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                if (finished.compareAndSet(false, true)) listener.onFailure(t)
            }
        })
        return object : Connection {
            override fun send(text: String): Boolean = webSocket.send(text)

            override fun close() {
                webSocket.close(NORMAL_CLOSURE, null)
            }

            override fun cancel() = webSocket.cancel()
        }
    }

    private companion object {
        const val NORMAL_CLOSURE = 1000
    }
}
