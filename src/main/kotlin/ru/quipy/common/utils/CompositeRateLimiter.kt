package ru.quipy.common.utils

import java.util.concurrent.CompletableFuture

class CompositeRateLimiter(
    private val rl1: RateLimiter,
    private val rl2: RateLimiter,
) : RateLimiter {
    override fun tick(): Boolean {
        return rl1.tick() && rl2.tick()
    }

    override fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10L)
        }
    }

    fun asyncTick(): CompletableFuture<Void> {
        return CompletableFuture.runAsync {
            tickBlocking()
        }
    }
}