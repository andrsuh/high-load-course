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
        TODO("Not yet implemented")
    }

    override fun tickBlocking(timeout: Duration): Boolean {
        TODO("Not yet implemented")
    }
}