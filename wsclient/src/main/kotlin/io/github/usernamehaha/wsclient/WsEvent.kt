package io.github.usernamehaha.wsclient

import kotlin.time.Duration

/**
 * 库内部发生的、值得记录的事情。每个事件的 `toString()` 都是可读的，可以直接打日志。
 *
 * 后续版本可能新增事件类型，`when` 请带上 `else` 分支。
 */
public sealed interface WsEvent {

    public class Connecting(public val url: String, public val attempt: Int) : WsEvent {
        override fun toString(): String = if (attempt == 0) "连接 $url" else "第 $attempt 次重连 $url"
    }

    public class Connected(public val url: String) : WsEvent {
        override fun toString(): String = "已连接 $url"
    }

    public class Disconnected(public val url: String, public val cause: DisconnectCause) : WsEvent {
        override fun toString(): String = "连接断开 $url：$cause"
    }

    public class ReconnectScheduled(public val attempt: Int, public val delay: Duration) : WsEvent {
        override fun toString(): String = "$delay 后进行第 $attempt 次重连"
    }

    /** [ReconnectPolicy] 返回了 null，不再重连。 */
    public class ReconnectAbandoned(public val cause: DisconnectCause) : WsEvent {
        override fun toString(): String = "放弃重连：$cause"
    }

    /** @property restored true 表示这是重连之后的自动恢复 */
    public class Subscribed(public val topic: String, public val restored: Boolean) : WsEvent {
        override fun toString(): String = if (restored) "恢复订阅 $topic" else "订阅 $topic"
    }

    public class Unsubscribed(public val topic: String) : WsEvent {
        override fun toString(): String = "退订 $topic"
    }

    /** 订阅或退订消息没能发出去（连接刚好断开或发送队列已满）。订阅会在下次重连后恢复。 */
    public class SendFailed(public val message: String) : WsEvent {
        override fun toString(): String = "发送失败：$message"
    }

    /**
     * 收集者处理不过来，消息被按 [Backpressure] 丢弃。同一个 topic 每秒最多上报一次。
     *
     * @property topic 为 null 表示 [WsClient.messages] 的收集者
     * @property count 距上次上报以来丢弃的条数
     */
    public class MessagesDropped(public val topic: String?, public val count: Long) : WsEvent {
        override fun toString(): String = "${topic ?: "messages"} 的收集者处理不过来，丢弃了 $count 条消息"
    }

    /** [WsConfig.topicOf]、[WsConfig.autoReply]、[WsConfig.binaryDecoder] 或 url 提供者抛了异常。 */
    public class CallbackFailed(public val name: String, public val cause: Throwable) : WsEvent {
        override fun toString(): String = "$name 抛出异常：$cause"
    }
}

/**
 * 接收 [WsEvent]。回调可能发生在任意线程，包括 OkHttp 的读线程，不要在里面做耗时操作。
 * 回调里抛出的异常会被丢弃。
 */
public fun interface WsListener {
    public fun onEvent(event: WsEvent)
}
