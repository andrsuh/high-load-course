package ru.quipy.common.utils

import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.max

/**
 * Smooth Rate Limiter - обеспечивает плавный, равномерный поток запросов без "рывков"
 *
 * Основан на принципе Leaky Bucket (Queue mode) из теории:
 * - Запросы "вытекают" с постоянной скоростью
 * - Между запросами соблюдается минимальный интервал = window / rate
 * - Исключает burst'ы (пачки запросов), делает трафик плавным
 */
class SmoothRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {

    companion object {
        private val logger = LoggerFactory.getLogger(SmoothRateLimiter::class.java)
    }

    // Минимальный интервал между запросами в миллисекундах
    private val minIntervalMs = window.toMillis().toDouble() / rate.toDouble()

    // Время когда был отправлен последний запрос (в миллисекундах)
    private val lastRequestTime = AtomicLong(0)

    private val lock = ReentrantLock()

    /**
     * Пытается получить разрешение на выполнение запроса.
     * Блокирует поток до тех пор, пока не наступит подходящее время для запроса.
     *
     * @return true если разрешение получено
     */
    override fun tick(): Boolean {
        lock.withLock {
            val now = System.currentTimeMillis()
            val lastTime = lastRequestTime.get()

            // Вычисляем когда можно отправить следующий запрос
            val nextAllowedTime = lastTime + minIntervalMs.toLong()

            if (now >= nextAllowedTime) {
                // Можем отправить сейчас
                lastRequestTime.set(now)
                return true
            }

            // Нужно подождать
            val waitTimeMs = nextAllowedTime - now

            if (waitTimeMs > 0) {
                try {
                    Thread.sleep(waitTimeMs)
                    lastRequestTime.set(System.currentTimeMillis())
                    return true
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return false
                }
            }

            lastRequestTime.set(now)
            return true
        }
    }

    /**
     * Пытается получить разрешение без блокировки.
     * Возвращает false если нужно подождать.
     */
    fun tryTick(): Boolean {
        lock.withLock {
            val now = System.currentTimeMillis()
            val lastTime = lastRequestTime.get()
            val nextAllowedTime = lastTime + minIntervalMs.toLong()

            if (now >= nextAllowedTime) {
                lastRequestTime.set(now)
                return true
            }

            return false
        }
    }

    /**
     * Возвращает время в миллисекундах до следующего доступного слота
     */
    fun timeUntilNextSlot(): Long {
        val now = System.currentTimeMillis()
        val lastTime = lastRequestTime.get()
        val nextAllowedTime = lastTime + minIntervalMs.toLong()
        return max(0, nextAllowedTime - now)
    }
}
