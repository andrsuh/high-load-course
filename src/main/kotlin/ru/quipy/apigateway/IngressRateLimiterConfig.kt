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

    @Value("\${ingress.rate-limiter.burst-capacity}")
    var burstCapacity: Int = 0

    @Bean
    fun ingressRateLimiter(meterRegistry: MeterRegistry): IngressRateLimiter {
        if (!ingressRateLimiterEnabled) {
            logger.warn("Ingress rate limiter is disabled: all incoming payment requests are admitted")
            return IngressRateLimiter(null, meterRegistry)
        }

        val limiter = createLimiter()
        logger.warn(
            "Ingress rate limiter enabled: type={}, rate={}, window={}s, burst={}, bucket={}",
            rateLimiterType, rate, windowSeconds, burstCapacity, bucketSize
        )
        return IngressRateLimiter(limiter, meterRegistry)
    }

    private fun createLimiter(): RateLimiter = when (rateLimiterType) {
        "sliding" -> SlidingWindowRateLimiter(rate.toLong(), Duration.ofSeconds(windowSeconds))

        "fixed" -> FixedWindowRateLimiter(rate, windowSeconds, TimeUnit.SECONDS)

        "token-bucket" -> TokenBucketRateLimiter(rate, burstCapacity, windowSeconds, TimeUnit.SECONDS)

        "leaking-bucket" -> LeakingBucketRateLimiter(rate.toLong(), Duration.ofSeconds(windowSeconds), bucketSize)

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
