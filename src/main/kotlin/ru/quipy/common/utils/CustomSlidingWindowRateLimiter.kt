package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.ConcurrentLinkedDeque

class CustomSlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration
): RateLimiter {

    private val queue = ConcurrentLinkedDeque<Long>()

    override fun tick(): Boolean {
        val currentTimeMillis = System.currentTimeMillis()

        while (!queue.isEmpty() && currentTimeMillis - queue.peekFirst() > window.toMillis()) {
            queue.pollFirst()
        }

        if (queue.size < rate) {
            queue.addLast(currentTimeMillis)
            return true
        }

        return false
    }
}