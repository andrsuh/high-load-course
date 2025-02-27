package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.RateLimiter
import java.time.Duration

@Configuration
class RateLimiterConfig {
    @Bean
    fun rateLimiter(): RateLimiter {
        return LeakingBucketRateLimiter(
            rate = 1,
            window = Duration.ofSeconds(1),
            bucketSize = 3
        )
    }
}