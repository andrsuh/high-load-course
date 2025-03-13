package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import java.time.Duration

@Configuration
class RateLimiterConfig {
    @Bean
    fun rateLimiter(): RateLimiter {
        return LeakingBucketRateLimiter(
            rate = 2,
            window = Duration.ofSeconds(1),
            bucketSize = 3
        )
    }
    @Bean
    fun ongoingWindow(): OngoingWindow {
        return OngoingWindow(4)
    }
}