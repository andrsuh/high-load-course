package ru.quipy.apigateway

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.*
import java.time.Duration
import java.util.concurrent.TimeUnit

@Configuration
class IngressRateLimiterConfig {

    @Value("\${ingress.rate-limiter.enabled:false}")
    var ingressRateLimiterEnabled: Boolean = false

    @Value("\${ingress.rate-limiter.type:sliding}")
    lateinit var rateLimiterType: String

    @Value("\${ingress.rate-limiter.rate:11}")
    var rate: Int = 11

    @Value("\${ingress.rate-limiter.window-seconds:1}")
    var windowSeconds: Long = 1

    @Value("\${ingress.rate-limiter.bucket-size:20}")
    var bucketSize: Int = 20

    @Value("\${ingress.rate-limiter.burst-capacity:15}")
    var burstCapacity: Int = 15

    @Bean
    fun ingressRateLimiter(): RateLimiter? {
        if (!ingressRateLimiterEnabled) {
            return null
        }

        return when (rateLimiterType) {
            "sliding" -> SlidingWindowRateLimiter(
                rate.toLong(),
                Duration.ofSeconds(windowSeconds)
            )
            "fixed" -> FixedWindowRateLimiter(
                rate,
                windowSeconds,
                TimeUnit.SECONDS
            )
            "token-bucket" -> TokenBucketRateLimiter(
                rate,
                burstCapacity,
                windowSeconds,
                TimeUnit.SECONDS
            )
            "leaking-bucket" -> LeakingBucketRateLimiter(
                rate.toLong(),
                Duration.ofSeconds(windowSeconds),
                bucketSize
            )
            "counting" -> CountingRateLimiter(
                rate,
                windowSeconds,
                TimeUnit.SECONDS
            )
            "composite-burst" -> {
                val burst = TokenBucketRateLimiter(rate, burstCapacity, 1, TimeUnit.SECONDS)
                val sustained = SlidingWindowRateLimiter(rate.toLong(), Duration.ofSeconds(windowSeconds))
                CompositeRateLimiter(burst, sustained)
            }
            else -> SlidingWindowRateLimiter(rate.toLong(), Duration.ofSeconds(windowSeconds))
        }
    }
}
