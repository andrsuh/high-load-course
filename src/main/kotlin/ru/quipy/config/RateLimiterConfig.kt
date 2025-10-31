package ru.quipy.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.RateLimiter
import java.time.Duration

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
}