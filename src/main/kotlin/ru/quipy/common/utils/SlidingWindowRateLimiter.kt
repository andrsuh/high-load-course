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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val count = AtomicLong()
    private val queue = PriorityBlockingQueue<Measure>(10_000)

    override fun tick(): Boolean {
        val permit = tryReserve() ?: return false
        permit.markSubmitted()
        return true
    }

    fun reserveBlockingUntil(deadline: Long): Permit? {
        while (true) {
            val remaining = deadline - System.currentTimeMillis()
            if (remaining <= 0) return null
            tryReserve()?.let { return it }
            Thread.sleep(minOf(10, remaining))
        }
    }

    private fun tryReserve(): Permit? {
        while (true) {
            val current = count.get()
            if (current >= rate) return null
            if (count.compareAndSet(current, current + 1)) return Permit()
        }
    }

    inner class Permit internal constructor() : AutoCloseable {
        private val completed = AtomicBoolean(false)

        fun markSubmitted() {
            check(completed.compareAndSet(false, true)) { "Rate permit already completed" }
            queue.add(Measure(1, System.currentTimeMillis()))
        }

        override fun close() {
            if (completed.compareAndSet(false, true)) count.decrementAndGet()
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
            count.decrementAndGet()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}
