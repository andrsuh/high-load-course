package ru.quipy.common.utils

import java.util.concurrent.atomic.AtomicLong

class PacedRateLimiter(private val intervalMs: Long) : RateLimiter {

    init {
        require(intervalMs > 0) { "intervalMs must be positive" }
    }

    private val nextSlotMs = AtomicLong(0L)

    override fun tick(): Boolean {
        val nowMs = System.currentTimeMillis()
        while (true) {
            val slot = nextSlotMs.get()
            if (nowMs < slot) return false
            if (nextSlotMs.compareAndSet(slot, nowMs + intervalMs)) return true
        }
    }
}
