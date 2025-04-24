package ru.quipy.payments.config

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.payments.properties.RateLimiterProperties
import java.time.Duration

@EnableConfigurationProperties(RateLimiterProperties::class)
@Configuration
class RateLimiterConfig {

    @Bean
    fun rateLimiter(rateLimiterProperties: RateLimiterProperties): RateLimiter = LeakingBucketRateLimiter(
        rate = rateLimiterProperties.rate,
        window = Duration.ofSeconds(rateLimiterProperties.window),
        bucketSize = rateLimiterProperties.bucketSize,
    )

    @Bean
    fun ongoingWindow(rateLimiterProperties: RateLimiterProperties): NonBlockingOngoingWindow
        = NonBlockingOngoingWindow(rateLimiterProperties.maxWinSize)
}