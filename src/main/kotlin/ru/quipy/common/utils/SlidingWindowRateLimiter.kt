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
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }

    private val lock = ReentrantLock()
    private val requests = mutableListOf<Long>()
    private val windowMs = window.toMillis()
    private var lastCleanup = System.currentTimeMillis()

    override fun tick(): Boolean {
        return lock.withLock {
            val now = System.currentTimeMillis()
            
            // Чистим прошлые запросы не после каждого нового, а после определённого времени - 100мс
            if (now - lastCleanup > 100 || windowMs > 5000) {
                val windowStart = now - windowMs
                requests.removeAll { it <= windowStart }
                lastCleanup = now
            }
            
            // Смотрим, можем ли добавить новый запрос
            if (requests.size < rate) {
                requests.add(now)
                true
            } else {
                false
            }
        }
    }

    override fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(1)
        }
    }

    override fun tickBlocking(timeout: Duration): Boolean {
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        while (System.currentTimeMillis() <= deadline) {
            if (tick()) return true
            Thread.sleep(1)
        }
        return false
    }
}