package io.github.usernamehaha.resocket

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel

/**
 * 收集者处理不过来时怎么办。每个收集者有自己的缓冲区，互不影响。
 *
 * 没有"挂起发送方"这个选项：消息是在 OkHttp 的读线程上分发的，挂起它等于停止读取，
 * 心跳应答也会跟着停，连接会被服务端踢掉。
 */
public class Backpressure private constructor(
    internal val capacity: Int,
    internal val overflow: BufferOverflow,
    private val description: String,
) {
    override fun toString(): String = description

    public companion object {
        /** 缓冲区满了丢最旧的。适合行情：新数据比旧数据有价值。默认策略，容量 64。 */
        public fun dropOldest(capacity: Int = 64): Backpressure {
            require(capacity > 0) { "capacity 必须大于 0，实际是 $capacity" }
            return Backpressure(capacity, BufferOverflow.DROP_OLDEST, "dropOldest($capacity)")
        }

        /** 缓冲区满了丢新到的。 */
        public fun dropNewest(capacity: Int = 64): Backpressure {
            require(capacity > 0) { "capacity 必须大于 0，实际是 $capacity" }
            return Backpressure(capacity, BufferOverflow.DROP_LATEST, "dropNewest($capacity)")
        }

        /** 只保留最新的一条。适合只关心当前值的场景，比如最新价。 */
        public fun latest(): Backpressure = Backpressure(1, BufferOverflow.DROP_OLDEST, "latest")

        /** 不丢消息，缓冲区无上限。适合 IM 这类不能丢消息的场景；收集者长期跟不上会耗尽内存。 */
        public fun unbounded(): Backpressure = Backpressure(Channel.UNLIMITED, BufferOverflow.SUSPEND, "unbounded")
    }
}
