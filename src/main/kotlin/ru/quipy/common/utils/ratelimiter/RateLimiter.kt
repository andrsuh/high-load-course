package ru.quipy.common.utils.ratelimiter

interface RateLimiter {
    fun tick(): Boolean

    fun tickBlocking(deadline: Long): Boolean {
        while (System.currentTimeMillis() < deadline && !tick()) {
            Thread.sleep(10)
        }

        return System.currentTimeMillis() < deadline
    }
}