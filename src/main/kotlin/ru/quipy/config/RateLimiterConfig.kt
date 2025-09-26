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
            rate = 100, // Высокий лимит для orders - не они bottleneck
            window = Duration.ofSeconds(1)
        )
    }

    @Bean
    fun paymentRateLimiter(): RateLimiter {
        return SlidingWindowRateLimiter(
            rate = 10, // Строгий лимит только для payments - 10 RPS как в acc-3
            window = Duration.ofSeconds(1)
        )
    }
}