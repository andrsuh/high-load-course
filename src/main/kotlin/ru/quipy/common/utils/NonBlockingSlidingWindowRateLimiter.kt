package ru.quipy.common.utils

import kotlinx.coroutines.*
import java.util.concurrent.atomic.AtomicLong
import java.time.Duration


class NonBlockingSlidingWindowRateLimiter(
    private val rate: Int,
    private val window: Duration = Duration.ofSeconds(1)
) {
    private val counter = AtomicLong(0)
    private val timestamps = java.util.concurrent.ConcurrentLinkedDeque<Long>()

    private val scope = CoroutineScope(
        Dispatchers.Default + SupervisorJob() + CoroutineName("NonBlockingSlidingWindowRateLimiter")
    )

    init {
        scope.launch {
            while (true) {
                delay(window.toMillis() / 4)
                cleanupExpired()
            }
        }
    }

    private fun cleanupExpired() {
        val threshold = System.currentTimeMillis() - window.toMillis()
        while (true) {
            val head = timestamps.peekFirst() ?: break
            if (head >= threshold) break
            timestamps.pollFirst()
            counter.decrementAndGet()
        }
    }

    fun tryAcquire(): Boolean {
        cleanupExpired()

        while (true) {
            val current = counter.get()
            if (current >= rate) return false

            if (counter.compareAndSet(current, current + 1)) {
                timestamps.addLast(System.currentTimeMillis())
                return true
            }
        }
    }

    suspend fun acquireSuspendWithTimeout(timeoutMs: Long): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (!tryAcquire()) {
            if (System.currentTimeMillis() >= deadline) return false
            delay(1)
        }
        return true
    }
}
