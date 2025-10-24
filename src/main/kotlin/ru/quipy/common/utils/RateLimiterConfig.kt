package ru.quipy.common.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class RateLimiterConfig {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(RateLimiterConfig::class.java)
    }

    @Value($$"${ratelimiter.rate}")
    private val rate: Long = -1

    @Value($$"${ratelimiter.bucket-size}")
    private val bucketSize: Int = -1

    @Bean
    fun incomingRateLimiter(): RateLimiter {
        LOGGER.info("Initialized incomingRateLimiter with rate: $rate and bucketSize: $bucketSize")

        return LeakingBucketRateLimiter(rate, Duration.ofSeconds(1), bucketSize)
    }
}