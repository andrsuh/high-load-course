package ru.quipy.common.utils

enum class CompositeMode {
    AND,
    OR
}

class CompositeRateLimiter(
    private val rl1: RateLimiter,
    private val rl2: RateLimiter,
    private val mode: CompositeMode = CompositeMode.OR
) : RateLimiter {
    override fun tick(): Boolean {
        return when (mode) {
            CompositeMode.AND -> rl1.tick() && rl2.tick()
            CompositeMode.OR -> {
                val r1 = rl1.tick()
                val r2 = rl2.tick()
                r1 || r2
            }
        }
    }
}
