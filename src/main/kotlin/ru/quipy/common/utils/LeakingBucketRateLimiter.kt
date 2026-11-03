package ru.quipy.common.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    private val bucketSize: Int,
) : RateLimiter {

    init {
        require(rate > 0) { "rate must be positive" }
        require(!window.isZero && !window.isNegative) { "window must be positive" }
        require(bucketSize > 0) { "bucketSize must be positive" }
    }

    /** Интервал между допусками: window / rate. */
    private val intervalNanos: Long = (window.toNanos() / rate).coerceAtLeast(1L)

    /** Запас на залп: bucketSize запросов проходят в пределах bucketSize-1 интервалов. */
    private val burstToleranceNanos: Long = intervalNanos * (bucketSize - 1)

    private val lock = Any()

    /** Теоретическое время следующего допуска (TAT в GCRA). */
    private var theoreticalArrivalNanos: Long = System.nanoTime()

    override fun tick(): Boolean = synchronized(lock) {
        val now = System.nanoTime()
        // Бакет пуст: допуск возможен, только если с прошлого допуска прошло достаточно времени.
        if (theoreticalArrivalNanos - now > burstToleranceNanos) {
            return false
        }
        theoreticalArrivalNanos = maxOf(theoreticalArrivalNanos, now) + intervalNanos
        return true
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)

        /**
         * Человекочитаемое описание конфигурации для лога старта.
         */
        fun describe(rate: Long, window: Duration, bucketSize: Int): String {
            val intervalMs = (window.toMillis() / rate).coerceAtLeast(1L)
            val effectiveRps = 1000.0 / intervalMs
            return "rate=$rate per $window, interval=$intervalMs ms (~%.2f rps), burst=$bucketSize"
                .format(effectiveRps)
        }
    }
}
