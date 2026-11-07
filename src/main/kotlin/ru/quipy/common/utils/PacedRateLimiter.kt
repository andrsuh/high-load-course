package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class PacedRateLimiter(rate: Long, window: Duration) {
    init { require(rate > 0); require(!window.isNegative && !window.isZero) }
    private val interval = (window.toNanos() + rate - 1) / rate
    private val lock = ReentrantLock(true)
    private var lastDispatch: Long? = null

    fun tickBlocking(timeout: Duration): Boolean {
        if (timeout.isNegative || timeout.isZero) return false
        val started = System.nanoTime()
        val budget = timeout.toNanos()
        if (!lock.tryLock(budget, TimeUnit.NANOSECONDS)) return false
        try {
            while (true) {
                val now = System.nanoTime()
                val remaining = budget - (now - started)
                if (remaining <= 0) return false
                val wait = lastDispatch?.let { interval - (now - it) } ?: 0L
                if (wait <= 0) {
                    lastDispatch = now
                    return true
                }
                TimeUnit.NANOSECONDS.sleep(minOf(wait, remaining))
            }
        } finally { lock.unlock() }
    }
}
