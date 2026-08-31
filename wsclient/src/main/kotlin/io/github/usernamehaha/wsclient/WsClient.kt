package io.github.usernamehaha.wsclient

import io.github.usernamehaha.wsclient.internal.OkHttpTransport
import io.github.usernamehaha.wsclient.internal.RealWsClient
import java.io.Closeable
import kotlin.time.TimeSource
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import okhttp3.OkHttpClient

/**
 * 带心跳、自动重连和订阅恢复的 WebSocket 客户端。所有方法线程安全且不阻塞。
 */
public interface WsClient : Closeable {
    public val state: StateFlow<ConnectionState>

    /**
     * 收到的全部文本消息，包括不属于任何订阅的。每个收集者有独立的缓冲区，使用 [WsConfig.backpressure]。
     * 收集是冷的：开始收集之后到达的消息才会收到。
     */
    public val messages: Flow<String>

    /** 开始连接并在断开后自动重连。已经在连接或已连接时什么都不做。 */
    public fun connect()

    /** 断开并停止重连。订阅关系保留，收集者不会结束，再次 [connect] 后自动恢复。 */
    public fun disconnect()

    /**
     * 正在等待重连时跳过剩余的等待，立即重连。适合在网络恢复、App 回到前台时调用。其他状态下什么都不做。
     */
    public fun reconnectNow()

    /**
     * 返回的 Flow 被收集时才订阅，收集取消时退订；同一个 topic 有多个收集者时只向服务端订阅一次。
     * 断线期间 Flow 不会结束也不会报错，只是没有数据，重连成功后库会重新发送订阅消息。
     * 需要感知断线（比如重建本地订单簿）请观察 [state]。
     *
     * [close] 之后 Flow 正常结束。
     */
    public fun subscribe(subscription: Subscription, backpressure: Backpressure? = null): Flow<String>

    /**
     * 发送一条文本消息。
     *
     * @return 未连接或 OkHttp 的发送队列已满时返回 false。库不会把消息留到重连后补发：
     * 对交易指令这类消息，延迟送达比送达失败更危险
     */
    public fun send(text: String): Boolean

    /** 断开连接，结束所有收集者，释放资源。之后不能再使用。 */
    override fun close()

    public companion object {
        /**
         * @param client 连接复用它的线程池、代理、证书等设置。建议传入 App 里已有的实例
         * @throws IllegalArgumentException 没有配置 url
         */
        public fun create(client: OkHttpClient = OkHttpClient(), configure: WsConfig.() -> Unit): WsClient {
            val config = WsConfig().apply(configure)
            requireNotNull(config.urlProvider) { "必须通过 url(...) 配置连接地址" }
            return RealWsClient(config, OkHttpTransport(client, config), TimeSource.Monotonic)
        }
    }
}
