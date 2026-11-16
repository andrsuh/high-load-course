package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.locks.LockSupport

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    private val bucketSize: Int,
) : RateLimiter {
    private val intervalNanos = (window.toNanos() + rate - 1) / rate + 1_000_000
    private var nextPermitAt = System.nanoTime()
    private var reservations = 0

    @Synchronized
    override fun tick(): Boolean {
        val now = System.nanoTime()
        if (now < nextPermitAt || reservations >= bucketSize) return false
        nextPermitAt = now + intervalNanos
        return true
    }

    fun reserveBlockingUntil(deadline: Long): Boolean {
        val waitNanos = synchronized(this) {
            val now = System.nanoTime()
            val permitAt = maxOf(now, nextPermitAt)
            val wait = permitAt - now
            if (System.currentTimeMillis() + Duration.ofNanos(wait).toMillis() >= deadline || reservations >= bucketSize) {
                return false
            }
            nextPermitAt = permitAt + intervalNanos
            reservations++
            wait
        }
        LockSupport.parkNanos(waitNanos)
        synchronized(this) { reservations-- }
        return true
    }
}
