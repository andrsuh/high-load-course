package ru.quipy.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.common.utils.RateLimiter
import java.time.Duration
import java.util.concurrent.TimeUnit

@Configuration
class RateLimiterConfig {

    @Bean
    fun orderRateLimiter(): RateLimiter {
        return SlidingWindowRateLimiter(
            rate = 100,
            window = Duration.ofSeconds(1)
        )
    }

    @Bean
    fun paymentRateLimiter(): RateLimiter {
        return SlidingWindowRateLimiter(
            rate = 10,
            window = Duration.ofSeconds(1)
        )
    }


    @Bean
    fun incomingPaymentRateLimiter(
        @Value("\${incoming.rate.strategy:SLIDING_WINDOW}") strategy: String,
        @Value("\${incoming.rate.limit:11}") rateLimit: Int,
        @Value("\${incoming.rate.bucket.capacity:110}") bucketCapacity: Int
    ): RateLimiter {
        return when (strategy.uppercase()) {
            "TOKEN_BUCKET" -> {
                TokenBucketRateLimiter(
                    rate = rateLimit,
                    bucketMaxCapacity = bucketCapacity,
                    window = 1,
                    timeUnit = TimeUnit.SECONDS
                )
            }
            else -> {
                SlidingWindowRateLimiter(
                    rate = rateLimit.toLong(),
                    window = Duration.ofSeconds(1)
                )
            }
        }
    }
}