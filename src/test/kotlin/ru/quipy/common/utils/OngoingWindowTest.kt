package ru.quipy.common.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class OngoingWindowTest {
    @Test
    fun `expired waiter does not consume capacity`() {
        val window = OngoingWindow(1)
        assertTrue(window.acquire(Duration.ofSeconds(1)))
        assertFalse(window.acquire(Duration.ofMillis(10)))
        assertFalse(window.acquire(Duration.ZERO))
        window.release()
        assertTrue(window.acquire(Duration.ofSeconds(1)))
        window.release()
    }

    @Test
    fun `waiting payment receives released capacity before a new arrival`() {
        val window = OngoingWindow(1)
        val executor = Executors.newSingleThreadExecutor()
        window.acquire()
        try {
            val waiting = executor.submit<Boolean> { window.acquire(Duration.ofSeconds(5)) }
            val end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
            while (window.awaitingQueueSize() == 0 && System.nanoTime() < end) Thread.yield()
            assertEquals(1, window.awaitingQueueSize())
            window.release()
            assertFalse(window.acquire(Duration.ofMillis(20)))
            assertTrue(waiting.get(2, TimeUnit.SECONDS))
            window.release()
            assertTrue(window.acquire(Duration.ofSeconds(1)))
            window.release()
        } finally {
            executor.shutdownNow()
        }
    }
}
