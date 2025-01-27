package ru.quipy.common.utils

class CompositeRateLimiter(
    private val rl1: RateLimiter,
    private val rl2: RateLimiter,
) : RateLimiter {
    override fun tick(): Boolean {
        return rl1.tick() && rl2.tick()
    }
}