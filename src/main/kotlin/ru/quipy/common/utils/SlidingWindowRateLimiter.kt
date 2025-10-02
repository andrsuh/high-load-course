package ru.quipy.common.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val lock = ReentrantLock()
    private val windowDurationMs = window.toMillis()
    private val requestTimestamps = mutableListOf<Long>()
    private var lastCleanupTime = System.currentTimeMillis()

    override fun tick(): Boolean {
        return lock.withLock {
            val now = System.currentTimeMillis()

            // Чистим каждые 100мс
            if (now - lastCleanupTime > 100) {
                val windowStart = now - windowDurationMs
                requestTimestamps.removeAll { it <= windowStart }
                lastCleanupTime = now
            }

            // Можем ли добавить новый запрос (помещаемся в окно)?
            if (requestTimestamps.size < rate) {
                requestTimestamps.add(now)
                true
            } else {
                false
            }
        }
    }

    fun tickBlocking() {
        // бесконечно ждем
        while (!tick()) {
            Thread.sleep(1)
        }
    }

    fun tickBlocking(timeout: Duration): Boolean {
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        // ждем добавления нового запроса пока не отвалимся по дедлайну
        while (System.currentTimeMillis() <= deadline) {
            if (tick()) return true
            Thread.sleep(1)
        }
        return false
    }
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}