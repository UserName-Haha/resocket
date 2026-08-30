package io.github.usernamehaha.wsclient

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * 心跳方式。TCP 连接在弱网下可能"半开"：本端以为连着，实际对端早已不可达，也不会有任何错误回调。
 * 心跳是发现这种情况的唯一办法。
 */
public sealed class Heartbeat {

    internal object None : Heartbeat()

    internal class ProtocolPing(val interval: Duration) : Heartbeat()

    internal class Text(val interval: Duration, val timeout: Duration, val message: () -> String) : Heartbeat()

    public companion object {
        /** 不发心跳，只依赖 TCP 层的错误。 */
        public fun none(): Heartbeat = None

        /**
         * WebSocket 协议层的 ping 帧，由 OkHttp 发送；下一次 ping 之前没收到 pong 就判定连接失效。
         * 服务端按协议自动应答，不需要业务配合，是默认方式。
         */
        public fun protocolPing(interval: Duration = 15.seconds): Heartbeat {
            require(interval.isPositive()) { "interval 必须大于 0，实际是 $interval" }
            return ProtocolPing(interval)
        }

        /**
         * 应用层心跳：每隔 [interval] 发一条文本消息，之后 [timeout] 内没收到任何数据就判定连接失效。
         * 用于服务端要求特定心跳格式、或者不应答 ping 帧的协议。
         *
         * 判活看的是"有没有收到数据"而不是"有没有收到特定的 pong"：行情推送本身就能证明连接活着，
         * 也省得库去理解各家的 pong 格式。
         *
         * @param message 每次发送前调用，便于带上时间戳或序号
         */
        public fun text(
            interval: Duration = 15.seconds,
            timeout: Duration = 10.seconds,
            message: () -> String,
        ): Heartbeat {
            require(interval.isPositive()) { "interval 必须大于 0，实际是 $interval" }
            require(timeout.isPositive()) { "timeout 必须大于 0，实际是 $timeout" }
            return Text(interval, timeout, message)
        }
    }
}
