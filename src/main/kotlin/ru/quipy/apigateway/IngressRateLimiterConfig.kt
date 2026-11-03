package ru.quipy.apigateway

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.*
import java.time.Duration
import java.util.concurrent.TimeUnit

@Configuration
class IngressRateLimiterConfig {

    companion object {
        private val logger = LoggerFactory.getLogger(IngressRateLimiterConfig::class.java)
    }

    @Value("\${ingress.rate-limiter.enabled}")
    var ingressRateLimiterEnabled: Boolean = false

    @Value("\${ingress.rate-limiter.type}")
    lateinit var rateLimiterType: String

    @Value("\${ingress.rate-limiter.rate}")
    var rate: Int = 0

    @Value("\${ingress.rate-limiter.window-seconds}")
    var windowSeconds: Long = 0L

    @Value("\${ingress.rate-limiter.bucket-size}")
    var bucketSize: Int = 0

    @Value("\${ingress.rate-limiter.long-bucket-size:256}")
    var longBucketSize: Int = 256

    @Value("\${ingress.rate-limiter.burst-capacity}")
    var burstCapacity: Int = 0

    /**
     * Два лимитера с ОДНИМ темпом (`rate`) и разными резервуарами: дедлайн запроса выбирает,
     * какой из них спросить (см. [IngressRateLimiter.tryAcquire]).
     *
     * Темп один намеренно: он задаёт запас относительно выходного лимита
     * аккаунта (11 rps). Вход держим на 1 rps ниже, чтобы очередь перед лимитером аккаунта
     * оставалась пустой и ожидание не съедало терпение клиента (см. комментарий у
     * `ingress.rate-limiter.rate`). Различаются только резервуары, потому что профили
     * принципиально разные:
     *   - короткий дедлайн (тест 1: 2.5 с, тест 2: 13 с) — ровный поток с волнами повторов
     *     после Retry-After, хватает `bucket-size` = 11;
     *   - длинный дедлайн (тест 3: 26 с) — пачки по 240–270 запросов раз в runits = 90 с,
     *     нужен `long-bucket-size` ≥ размера пачки.
     *
     */
    @Bean
    fun ingressRateLimiter(meterRegistry: MeterRegistry): IngressRateLimiter {
        if (!ingressRateLimiterEnabled) {
            logger.warn("Ingress rate limiter is disabled: all incoming payment requests are admitted")
            return IngressRateLimiter(shortDeadlineDelegate = null, meterRegistry = meterRegistry)
        }

        val shortLimiter = createLimiter(bucketSize)
        val longLimiter = createLimiter(longBucketSize)
        logger.warn(
            "Ingress rate limiter enabled: type={}, rate={}, window={}s, " +
                "bucket(short deadline)={}, bucket(long deadline)={}",
            rateLimiterType, rate, windowSeconds, bucketSize, longBucketSize
        )
        return IngressRateLimiter(
            shortDeadlineDelegate = shortLimiter,
            longDeadlineDelegate = longLimiter,
            meterRegistry = meterRegistry,
        )
    }

    private fun createLimiter(configuredBucketSize: Int): RateLimiter = when (rateLimiterType) {
        "sliding" -> SlidingWindowRateLimiter(rate.toLong(), Duration.ofSeconds(windowSeconds))

        "fixed" -> FixedWindowRateLimiter(rate, windowSeconds, TimeUnit.SECONDS)

        "token-bucket" -> TokenBucketRateLimiter(rate, maxOf(burstCapacity, configuredBucketSize), windowSeconds, TimeUnit.SECONDS)

        "leaking-bucket" -> LeakingBucketRateLimiter(rate.toLong(), Duration.ofSeconds(windowSeconds), configuredBucketSize)

        "counting" -> CountingRateLimiter(rate, windowSeconds, TimeUnit.SECONDS)

        "composite-burst" -> CompositeRateLimiter(
            SlidingWindowRateLimiter(rate.toLong(), Duration.ofSeconds(windowSeconds)),
            TokenBucketRateLimiter(rate, burstCapacity, windowSeconds, TimeUnit.SECONDS),
        )

        "paced" -> {
            val intervalMs = ((windowSeconds * 1000 + rate - 1) / rate) + 1
            logger.warn(
                "Ingress rate limiter paced: rate={} per {}s -> interval={} ms (~{} rps)",
                rate, windowSeconds, intervalMs,
                if (intervalMs > 0) 1000 / intervalMs else 0
            )
            PacedRateLimiter(intervalMs)
        }

        else -> {
            logger.warn("Unknown ingress rate limiter type '{}', falling back to sliding window", rateLimiterType)
            SlidingWindowRateLimiter(rate.toLong(), Duration.ofSeconds(windowSeconds))
        }
    }
}
