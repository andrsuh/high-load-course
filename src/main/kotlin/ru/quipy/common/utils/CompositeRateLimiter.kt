package ru.quipy.common.utils

import java.time.Duration

class CompositeRateLimiter(
    private val rl1: RateLimiter,
    private val rl2: RateLimiter,
) : RateLimiter {
    override fun tick(): Boolean {
        return rl1.tick() && rl2.tick()
    }

    override fun tickBlocking() {
        rl1.tickBlocking()
        rl2.tickBlocking()
    }

    override fun tickBlocking(timeout: Duration): Boolean {
        val half = Duration.ofMillis(timeout.toMillis() / 2)
        val first = rl1.tickBlocking(half)
        if (!first) return false
        val remaining = Duration.ofMillis(timeout.toMillis() - half.toMillis())
        return rl2.tickBlocking(remaining)
    }
}
