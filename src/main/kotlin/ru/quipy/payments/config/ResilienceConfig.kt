package ru.quipy.payments.config

import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.PaymentAccountProperties
import java.time.Duration


@Configuration
class ResilienceConfig {

    @Bean
    fun rateLimiters(
        properties: List<PaymentAccountProperties>,
    ): List<RateLimiter> = properties.map {
        val config = RateLimiterConfig.custom()
            .timeoutDuration(it.averageProcessingTime)
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .limitForPeriod(it.rateLimitPerSec)
            .build()

        val registry = RateLimiterRegistry.of(config)
        val rateLimiter = registry.rateLimiter(it.accountName)
        rateLimiter
    }
}
