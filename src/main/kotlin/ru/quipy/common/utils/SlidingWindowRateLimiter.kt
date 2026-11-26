package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.PriorityQueue
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val sum = AtomicLong(0)
    private val queue = PriorityBlockingQueue<Measure>(10_000)

    override fun tick(): Boolean {
        while (true) {
            val curSum = sum.get()
            if (curSum >= rate) return false
            if (sum.compareAndSet(curSum, curSum + 1)) {
                queue.add(Measure(1, System.currentTimeMillis()))
                return true
            }
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    fun estimatedDelayMillis(permits: Int): Long {
        if (permits <= 0) return 0
        val now = System.currentTimeMillis()
        val releases = PriorityQueue<Long>()
        val recorded = queue.toList().sortedByDescending { it.timestamp }.take(rate.toInt())
        recorded.forEach { releases.add(maxOf(now, it.timestamp + window.toMillis())) }
        val occupied = maxOf(recorded.size, sum.get().toInt()).coerceAtMost(rate.toInt())
        repeat(occupied - recorded.size) { releases.add(now + window.toMillis()) }
        repeat(rate.toInt() - occupied) { releases.add(now) }
        var availableAt = now
        repeat(permits) {
            availableAt = releases.remove()
            releases.add(availableAt + window.toMillis())
        }
        return availableAt - now
    }

    fun tickBlocking(timeoutMillis: Long): Boolean {
        if (timeoutMillis <= 0) return false
        val waitUntil = System.currentTimeMillis() + timeoutMillis
        while (!tick()) {
            val left = waitUntil - System.currentTimeMillis()
            if (left <= 0) return false
            Thread.sleep(minOf(10, left))
        }
        return true
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
}
