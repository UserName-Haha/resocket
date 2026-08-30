package io.github.usernamehaha.wsclient.internal

import io.github.usernamehaha.wsclient.Backpressure
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.ComparableTimeMark
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel

/** 一个收集者的缓冲区。写入发生在 OkHttp 的读线程，读取发生在收集者的协程。 */
internal class Sink(
    val topic: String?,
    backpressure: Backpressure,
    private val timeSource: TimeSource.WithComparableMarks,
    private val collector: Job?,
    private val onDropped: (topic: String?, count: Long) -> Unit,
) {
    private val dropped = AtomicLong()

    @Volatile
    private var lastReportedAt: ComparableTimeMark? = null

    @Volatile
    private var detached = false

    /**
     * onUndeliveredElement 不只在缓冲区溢出时触发：收集者被取消时，已经交到它手上但还没来得及处理的那一条、
     * 以及缓冲区里剩下的，也都会走到这里。那些不是背压造成的丢弃，不应该上报。
     */
    val channel: Channel<String> = Channel(backpressure.capacity, backpressure.overflow) {
        if (!detached && collector?.isActive != false) recordDrop()
    }

    fun offer(message: String) {
        channel.trySend(message)
    }

    fun detach() {
        detached = true
        channel.cancel()
    }

    /** 正常结束：收集者读完缓冲区里已有的消息后退出。 */
    fun complete() {
        detached = true
        channel.close()
    }

    private fun recordDrop() {
        dropped.incrementAndGet()
        val last = lastReportedAt
        // 高频行情下一秒可能丢成百上千条，逐条上报会让监听器本身成为瓶颈
        if (last != null && last.elapsedNow() < REPORT_INTERVAL) return
        lastReportedAt = timeSource.markNow()
        onDropped(topic, dropped.getAndSet(0))
    }

    private companion object {
        val REPORT_INTERVAL = 1.seconds
    }
}
