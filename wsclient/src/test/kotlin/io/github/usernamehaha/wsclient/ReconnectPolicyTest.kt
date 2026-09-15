package io.github.usernamehaha.wsclient

import java.io.IOException
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class ReconnectPolicyTest {
    private val cause = DisconnectCause.Failure(IOException())

    @Test
    fun `延迟按指数增长并封顶`() {
        val policy = ReconnectPolicy.exponentialBackoff(initialDelay = 1.seconds, maxDelay = 30.seconds, jitter = 0.0)
        assertEquals(
            listOf(1, 2, 4, 8, 16, 30, 30).map { it.seconds },
            (1..7).map { policy.nextDelay(it, cause) },
        )
    }

    @Test
    fun `重连次数很大时不会溢出`() {
        val policy = ReconnectPolicy.exponentialBackoff(jitter = 0.0)
        assertEquals(30.seconds, policy.nextDelay(10_000, cause))
        assertEquals(30.seconds, policy.nextDelay(Int.MAX_VALUE, cause))
    }

    @Test
    fun `抖动让延迟落在上下浮动的范围内，而且不是常数`() {
        val policy = ReconnectPolicy.exponentialBackoff(
            initialDelay = 10.seconds, maxDelay = 60.seconds, multiplier = 1.0, jitter = 0.2,
            maxAttempts = null, random = Random(42),
        )
        val delays = (1..200).map { policy.nextDelay(it, cause)!! }
        assertTrue(delays.all { it in 8.seconds..12.seconds })
        assertTrue(delays.max() - delays.min() > 500.milliseconds)
    }

    @Test
    fun `抖动之后也不会超过 maxDelay`() {
        val policy = ReconnectPolicy.exponentialBackoff(
            initialDelay = 1.seconds, maxDelay = 30.seconds, multiplier = 2.0, jitter = 0.5,
            maxAttempts = null, random = Random(7),
        )
        val delays = (1..200).map { policy.nextDelay(it, cause)!! }
        assertTrue(delays.all { it <= 30.seconds })
        assertTrue(delays.drop(10).any { it < 30.seconds })
    }

    @Test
    fun `超过最大次数后放弃`() {
        val policy = ReconnectPolicy.exponentialBackoff(jitter = 0.0, maxAttempts = 2)
        assertEquals(1.seconds, policy.nextDelay(1, cause))
        assertEquals(2.seconds, policy.nextDelay(2, cause))
        assertNull(policy.nextDelay(3, cause))
    }

    @Test
    fun `never 不重连`() {
        assertNull(ReconnectPolicy.never().nextDelay(1, cause))
    }

    @Test
    fun `参数校验`() {
        assertThrows(IllegalArgumentException::class.java) { ReconnectPolicy.exponentialBackoff(initialDelay = 0.seconds) }
        assertThrows(IllegalArgumentException::class.java) {
            ReconnectPolicy.exponentialBackoff(initialDelay = 5.seconds, maxDelay = 1.seconds)
        }
        assertThrows(IllegalArgumentException::class.java) { ReconnectPolicy.exponentialBackoff(multiplier = 0.5) }
        assertThrows(IllegalArgumentException::class.java) { ReconnectPolicy.exponentialBackoff(jitter = 1.5) }
        assertThrows(IllegalArgumentException::class.java) { ReconnectPolicy.exponentialBackoff(maxAttempts = 0) }
    }
}
