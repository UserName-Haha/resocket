package io.github.usernamehaha.wsclient

import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** 断开之后要不要重连、等多久。 */
public fun interface ReconnectPolicy {
    /**
     * @param attempt 从 1 开始，连接稳定一段时间（[WsConfig.stableAfter]）后重新计数
     * @return 等待多久后重连；返回 null 表示放弃，状态回到 [ConnectionState.Disconnected]
     */
    public fun nextDelay(attempt: Int, cause: DisconnectCause): Duration?

    public companion object {
        /**
         * 指数退避：第 n 次重连等待 `initialDelay * multiplier^(n-1)`，乘以 `1 ± jitter` 范围内的随机系数，
         * 结果不超过 [maxDelay]。
         *
         * 抖动是为了避免服务端重启后所有客户端在同一时刻连回来。
         *
         * @param maxAttempts 连续失败多少次后放弃；null 表示一直重试
         */
        public fun exponentialBackoff(
            initialDelay: Duration = 1.seconds,
            maxDelay: Duration = 30.seconds,
            multiplier: Double = 2.0,
            jitter: Double = 0.2,
            maxAttempts: Int? = null,
        ): ReconnectPolicy = exponentialBackoff(initialDelay, maxDelay, multiplier, jitter, maxAttempts, Random.Default)

        internal fun exponentialBackoff(
            initialDelay: Duration,
            maxDelay: Duration,
            multiplier: Double,
            jitter: Double,
            maxAttempts: Int?,
            random: Random,
        ): ReconnectPolicy {
            require(initialDelay.isPositive()) { "initialDelay 必须大于 0，实际是 $initialDelay" }
            require(maxDelay >= initialDelay) { "maxDelay 不能小于 initialDelay" }
            require(multiplier >= 1.0) { "multiplier 不能小于 1，实际是 $multiplier" }
            require(jitter in 0.0..1.0) { "jitter 应在 0 到 1 之间，实际是 $jitter" }
            require(maxAttempts == null || maxAttempts > 0) { "maxAttempts 必须大于 0，实际是 $maxAttempts" }
            return ReconnectPolicy { attempt, _ ->
                if (maxAttempts != null && attempt > maxAttempts) return@ReconnectPolicy null
                // 指数先在 Double 上算并封顶，attempt 很大时不会溢出
                val factor = Math.pow(multiplier, (attempt - 1).coerceAtLeast(0).toDouble())
                val base = if (factor.isInfinite() || initialDelay * factor > maxDelay) maxDelay else initialDelay * factor
                val scale = if (jitter == 0.0) 1.0 else 1.0 + (random.nextDouble() * 2 - 1) * jitter
                // 抖动之后再封一次顶：调用方配置 maxDelay 时期望的是一个硬上限
                (base * scale).coerceAtMost(maxDelay)
            }
        }

        /** 不重连。 */
        public fun never(): ReconnectPolicy = ReconnectPolicy { _, _ -> null }
    }
}
