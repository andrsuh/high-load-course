package ru.quipy.common.utils

import java.util.concurrent.TimeUnit

class PacedRateLimiter(
    rate: Int,
    safetyFactor: Double = 1.05,
    private val timeUnit: TimeUnit = TimeUnit.SECONDS,
) {
    private val intervalNanos = (timeUnit.toNanos(1) / rate.coerceAtLeast(1) * safetyFactor).toLong()
    private var nextSlotNanos = System.nanoTime()

    @Synchronized
    fun tick(): Boolean {
        val now = System.nanoTime()
        if (now < nextSlotNanos) {
            return false
        }
        nextSlotNanos = now + intervalNanos
        return true
    }

    @Synchronized
    fun nanosUntilNextSlot(): Long {
        val remaining = nextSlotNanos - System.nanoTime()
        return if (remaining > 0) remaining else 0L
    }
}
