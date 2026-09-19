package io.github.usernamehaha.resocket

import kotlin.time.Duration

/**
 * 连接状态机。
 *
 * ```
 * Disconnected --connect()--> Connecting --> Connected
 *                                 |              |
 *                                 v              v
 *                          WaitingToReconnect <--+     （断开后按 ReconnectPolicy 等待）
 *                                 |
 *                                 +--> Connecting ...
 * 任意状态 --disconnect()--> Disconnected
 * 任意状态 --close()------> Closed（终态）
 * ```
 */
public sealed interface ConnectionState {

    /**
     * 没有连接，也不会自动去连。
     *
     * @property cause 为 null 表示还没连过或者是调用方主动断开；否则是 [ReconnectPolicy] 放弃重连时的断开原因
     */
    public class Disconnected(public val cause: DisconnectCause? = null) : ConnectionState {
        override fun equals(other: Any?): Boolean = other is Disconnected && other.cause == cause
        override fun hashCode(): Int = cause.hashCode()
        override fun toString(): String = "Disconnected(cause=$cause)"
    }

    /** @property attempt 0 表示首次连接，之后是第几次重连 */
    public class Connecting(public val attempt: Int) : ConnectionState {
        override fun equals(other: Any?): Boolean = other is Connecting && other.attempt == attempt
        override fun hashCode(): Int = attempt
        override fun toString(): String = "Connecting(attempt=$attempt)"
    }

    public object Connected : ConnectionState {
        override fun toString(): String = "Connected"
    }

    public class WaitingToReconnect(
        public val attempt: Int,
        public val delay: Duration,
        public val cause: DisconnectCause,
    ) : ConnectionState {
        override fun equals(other: Any?): Boolean =
            other is WaitingToReconnect && other.attempt == attempt && other.delay == delay && other.cause == cause

        override fun hashCode(): Int = 31 * (31 * attempt + delay.hashCode()) + cause.hashCode()
        override fun toString(): String = "WaitingToReconnect(attempt=$attempt, delay=$delay, cause=$cause)"
    }

    /** [ReSocket.close] 之后的终态。 */
    public object Closed : ConnectionState {
        override fun toString(): String = "Closed"
    }
}

/** 连接为什么断了。 */
public sealed interface DisconnectCause {

    /** 连接失败或异常中断，包括心跳超时（[error] 是 [HeartbeatTimeoutException]）。 */
    public class Failure(public val error: Throwable) : DisconnectCause {
        override fun equals(other: Any?): Boolean = other is Failure && other.error === error
        override fun hashCode(): Int = System.identityHashCode(error)
        override fun toString(): String = "Failure(${error.javaClass.simpleName}: ${error.message})"
    }

    /** 服务端发起了正常的关闭握手。 */
    public class Closed(public val code: Int, public val reason: String) : DisconnectCause {
        override fun equals(other: Any?): Boolean = other is Closed && other.code == code && other.reason == reason
        override fun hashCode(): Int = 31 * code + reason.hashCode()
        override fun toString(): String = "Closed(code=$code, reason=$reason)"
    }
}

/** 发出心跳后在超时时间内没有收到任何数据。 */
public class HeartbeatTimeoutException(message: String) : java.io.IOException(message)
