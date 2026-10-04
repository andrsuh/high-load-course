package ru.quipy.common.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class PaymentLimitsTest {
    @Test
    fun `concurrent request slot is released for the next waiter`() {
        val window = OngoingWindow(1)
        assertTrue(window.tryAcquire(0))
        assertFalse(window.tryAcquire(25))
        val executor = Executors.newSingleThreadExecutor()
        try {
            val waiting = executor.submit<Boolean> { window.tryAcquire(1_000) }
            window.release()
            assertTrue(waiting.get(2, TimeUnit.SECONDS))
            assertFalse(window.tryAcquire(0))
            window.release()
            assertTrue(window.tryAcquire(0))
            window.release()
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `expired deadline consumes no rate permit`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofMillis(300))
        assertFalse(limiter.tickBlockingUntil(System.currentTimeMillis() - 1))
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
    }

    @Test
    fun `rate wait ends at the deadline instead of waiting for window expiry`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofSeconds(5))
        assertTrue(limiter.tick())
        val started = System.nanoTime()
        assertFalse(limiter.tickBlockingUntil(System.currentTimeMillis() + 50))
        assertTrue(Duration.ofNanos(System.nanoTime() - started).toMillis() < 1_000)
    }

    @Test
    fun `rate permit becomes available after window expiry`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofMillis(100))
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
        assertTrue(limiter.tickBlockingUntil(System.currentTimeMillis() + 2_000))
        assertFalse(limiter.tick())
    }

    @Test
    fun `rate reservation does not expire while waiting for a semaphore`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofMillis(50))
        val window = OngoingWindow(1)
        assertTrue(window.tryAcquire(0))
        val permit = requireNotNull(limiter.reserveBlockingUntil(System.currentTimeMillis() + 1_000))
        try {
            // Wait longer than the rate window behind an occupied concurrency slot.
            assertFalse(window.tryAcquire(150))
            assertNull(limiter.reserveBlockingUntil(System.currentTimeMillis() + 50))
            window.release()
            assertTrue(window.tryAcquire(0))
            permit.markSubmitted()
            assertFalse(limiter.tick())
            assertTrue(limiter.tickBlockingUntil(System.currentTimeMillis() + 2_000))
            window.release()
        } finally {
            permit.close()
        }
    }

    @Test
    fun `abandoned reservation returns exactly one permit without an HTTP call`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofSeconds(5))
        val permit = requireNotNull(limiter.reserveBlockingUntil(System.currentTimeMillis() + 1_000))
        assertFalse(limiter.tick())
        permit.close()
        permit.close()
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
    }

    @Test
    fun `closing a submitted reservation cannot release its permit early`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofSeconds(5))
        val permit = requireNotNull(limiter.reserveBlockingUntil(System.currentTimeMillis() + 1_000))
        permit.markSubmitted()
        permit.close()
        permit.close()
        assertFalse(limiter.tick())
    }
}
