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
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val lock = ReentrantLock()
    private val windowDurationMs = window.toMillis()
    private val requestTimestamps = mutableListOf<Long>()
    private var lastCleanupTime = System.currentTimeMillis()
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val sum = AtomicLong(0)
    private val queue = PriorityBlockingQueue<Measure>(10_000)

    override fun tick(): Boolean {
        return lock.withLock {
            val now = System.currentTimeMillis()
            val windowStart = now - windowDurationMs

            cleanupOldRequests(now, windowStart)
            if (requestTimestamps.size < rate) {
                requestTimestamps.add(now)
                true
            } else {
                false
            }
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
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
            sum.addAndGet(-1)
            queue.take()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }

    private fun cleanupOldRequests(now: Long, windowStart: Long) {
        if (now - lastCleanupTime <= 50) return

        val iterator = requestTimestamps.iterator()
        var removedCount = 0

        while (iterator.hasNext()) {
            val timestamp = iterator.next()
            if (timestamp <= windowStart) {
                iterator.remove()
                removedCount++
            } else {
                break
            }
        }

        lastCleanupTime = now

        if (removedCount > 0) {
            logger.trace("Cleaned up $removedCount old requests from sliding window")
        }
    }

}