package ru.quipy.common.utils.ratelimiter

import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import java.time.Duration
import java.util.concurrent.TimeUnit

class ResilienceRateLimiter(
    accountName: String,
    rate: Int,
    timeUnit: TimeUnit = TimeUnit.SECONDS
) : RateLimiter {

    private val rateLimiter: io.github.resilience4j.ratelimiter.RateLimiter

    init {
        val config = RateLimiterConfig.custom()
            .limitRefreshPeriod(if (timeUnit == TimeUnit.SECONDS) Duration.ofSeconds(1) else Duration.ofMinutes(1))
            .limitForPeriod(rate)
            .timeoutDuration(Duration.ofMillis(5))
            .build()

        val rateLimiterRegistry = RateLimiterRegistry.of(config)

        rateLimiter = rateLimiterRegistry.rateLimiter(accountName)
    }

    override fun tick(): Boolean {
        val sleepMillis = rateLimiter.reservePermission()
        if (sleepMillis > 0) {
            Thread.sleep(sleepMillis)
            return true
        } else {
            return false
        }
    }
}
