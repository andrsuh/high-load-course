package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
) : RateLimiter {
    private val windowNanos = window.toNanos()
    private val lock = ReentrantLock(true)
    private val timestamps = ArrayDeque<Long>()

    init {
        require(rate > 0)
        require(windowNanos > 0)
    }

    private fun acquireOrWait(now: Long): Long {
        while (timestamps.isNotEmpty() && now - timestamps.first >= windowNanos) {
            timestamps.removeFirst()
        }
        if (timestamps.size.toLong() < rate) {
            timestamps.addLast(now)
            return 0
        }
        return windowNanos - (now - timestamps.first)
    }

    override fun tick(): Boolean {
        if (!lock.tryLock(0, TimeUnit.NANOSECONDS)) return false
        try { return acquireOrWait(System.nanoTime()) == 0L }
        finally { lock.unlock() }
    }

    fun tickBlocking() {
        lock.lockInterruptibly()
        try {
            while (true) {
                val wait = acquireOrWait(System.nanoTime())
                if (wait == 0L) return
                TimeUnit.NANOSECONDS.sleep(wait)
            }
        } finally { lock.unlock() }
    }

    fun tickBlocking(timeout: Duration): Boolean {
        if (timeout.isNegative || timeout.isZero) return false
        val startedAt = System.nanoTime()
        val budget = timeout.toNanos()
        if (!lock.tryLock(budget, TimeUnit.NANOSECONDS)) return false
        try {
            while (true) {
                val now = System.nanoTime()
                val remaining = budget - (now - startedAt)
                if (remaining <= 0) return false
                val wait = acquireOrWait(now)
                if (wait == 0L) return true
                TimeUnit.NANOSECONDS.sleep(minOf(wait, remaining))
            }
        } finally { lock.unlock() }
    }
}
