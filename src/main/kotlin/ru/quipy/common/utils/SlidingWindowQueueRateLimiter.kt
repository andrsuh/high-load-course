package ru.quipy.common.utils

import java.util.*

class SlidingWindowQueueRateLimiter(private val maxRequests: Int) {
    private val queue: ArrayDeque<Long> = ArrayDeque()
    private val windowMillis = 1000L

    @Synchronized
    fun allowRequest(): Boolean {
        val now = System.currentTimeMillis()
        while (queue.isNotEmpty() && now - queue.peekFirst() > windowMillis) {
            queue.pollFirst()
        }
        if (queue.size < maxRequests) {
            queue.addLast(now)
            return true
        }
        return false
    }
}