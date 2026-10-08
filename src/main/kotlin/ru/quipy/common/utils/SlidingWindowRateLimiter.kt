package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val permits = Semaphore(Math.toIntExact(rate), true)
    private val queue = PriorityBlockingQueue<Measure>(10_000)

    override fun tick(): Boolean {
        if (!permits.tryAcquire()) return false
        Permit().markSubmitted()
        return true
    }

    fun reserveBlockingUntil(deadline: Long): Permit? {
        val remaining = deadline - System.currentTimeMillis()
        if (remaining <= 0 || !permits.tryAcquire(remaining, TimeUnit.MILLISECONDS)) return null
        return Permit()
    }

    inner class Permit internal constructor() : AutoCloseable {
        private val completed = AtomicBoolean(false)

        fun markSubmitted() {
            check(completed.compareAndSet(false, true)) { "Rate permit already completed" }
            queue.add(Measure(1, System.currentTimeMillis()))
        }

        override fun close() {
            if (completed.compareAndSet(false, true)) permits.release()
        }
    }

    data class Measure(
        val value: Long,
        val timestamp: Long
    ) : Comparable<Measure> {
        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val head = queue.peek()
            val winStart = System.currentTimeMillis() - window.toMillis()
            if (head == null) {
                delay(1L)
                continue
            }
            if (head.timestamp > winStart) {
                delay(head.timestamp - winStart)
                continue
            }
            queue.take()
            permits.release()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}
