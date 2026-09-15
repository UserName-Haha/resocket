package io.github.usernamehaha.wsclient

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import okhttp3.Request
import okio.ByteString

/** [WsClient.create] 的配置。除了 url 以外都有默认值。 */
public class WsConfig internal constructor() {
    internal var urlProvider: (() -> String)? = null

    public fun url(url: String) {
        urlProvider = { url }
    }

    /** 每次（重新）连接前调用，可以在这里换线路或者带上新的签名参数。 */
    public fun url(provider: () -> String) {
        urlProvider = provider
    }

    public var heartbeat: Heartbeat = Heartbeat.protocolPing()

    public var reconnectPolicy: ReconnectPolicy = ReconnectPolicy.exponentialBackoff()

    /**
     * 连接保持这么久才算稳定，重连计数清零。
     *
     * 不在连上的那一刻清零，是因为有一类故障是"能连上但马上被踢"（鉴权失败、服务端过载），
     * 那样的话退避永远停在第一档，等于没有退避。
     */
    public var stableAfter: Duration = 10.seconds

    /**
     * 从消息里取出 topic，用来把消息分发给对应的订阅。返回 null 表示这条消息不属于任何订阅
     * （心跳应答、订阅确认等），它只会出现在 [WsClient.messages] 里。
     *
     * 每条消息调用一次，在 OkHttp 的读线程上执行，应当尽量轻。不设置的话 [WsClient.subscribe] 收不到任何消息。
     */
    public var topicOf: ((message: String) -> String?)? = null

    /**
     * 服务端主动发心跳、要求客户端应答的协议用它：返回要回复的内容，不需要回复返回 null。
     * 在 OkHttp 的读线程上执行。
     */
    public var autoReply: ((message: String) -> String?)? = null

    /** 把二进制帧解码成文本，比如 gzip 压缩的行情。不设置的话二进制帧被忽略。 */
    public var binaryDecoder: ((bytes: ByteString) -> String?)? = null

    /**
     * 每次连接成功后、恢复订阅之前要发送的消息，比如登录。每次连接都会重新调用。
     * 库不等待这些消息的应答。
     */
    public var greeting: (() -> List<String>)? = null

    /** 给握手请求加 header 等。 */
    public var configureRequest: ((Request.Builder) -> Unit)? = null

    /** [WsClient.subscribe] 和 [WsClient.messages] 的默认背压策略。 */
    public var backpressure: Backpressure = Backpressure.dropOldest()

    internal var listener: WsListener? = null

    /**
     * 接收库内部的事件，不设置就完全静默。
     *
     * 做成函数而不是属性，是因为 Kotlin 只对函数参数做 SAM 转换：这样可以直接写 `listener { event -> ... }`。
     */
    public fun listener(listener: WsListener) {
        this.listener = listener
    }
}
