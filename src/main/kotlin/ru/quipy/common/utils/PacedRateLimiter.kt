package ru.quipy.common.utils

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class PacedRateLimiter(ratePerSecond: Int) {
    init {
        require(ratePerSecond > 0) { "Rate must be positive" }
    }

    private val intervalNanos = (1_100_000_000L + ratePerSecond - 1) / ratePerSecond
    private val lock = ReentrantLock(true)
    private var nextPermitNanos = System.nanoTime()

    @Throws(InterruptedException::class)
    fun acquire(deadline: Long): Boolean {
        val remainingMillis = deadline - System.currentTimeMillis()
        if (remainingMillis <= 0 || !lock.tryLock(remainingMillis, TimeUnit.MILLISECONDS)) return false
        try {
            while (true) {
                val remaining = deadline - System.currentTimeMillis()
                if (remaining <= 0) return false
                val now = System.nanoTime()
                val waitNanos = nextPermitNanos - now
                if (waitNanos <= 0) {
                    nextPermitNanos = now + intervalNanos
                    return true
                }
                if (waitNanos >= TimeUnit.MILLISECONDS.toNanos(remaining)) return false
                TimeUnit.NANOSECONDS.sleep(waitNanos)
            }
        } finally {
            lock.unlock()
        }
    }
}
