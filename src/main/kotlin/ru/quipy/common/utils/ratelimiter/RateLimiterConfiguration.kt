package ru.quipy.common.utils.ratelimiter

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.PaymentAccountProperties
import java.time.Duration

@Configuration
class RateLimiterConfiguration {

    @Bean
    fun rateLimiters(
        properties: List<PaymentAccountProperties>,
    ): Map<String, RateLimiter?> =
        properties
            .associateBy(PaymentAccountProperties::accountName)
            .mapValues {
                when (it.key) {
                    "acc-3" -> SlidingWindowRateLimiter(it.value.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
                    "acc-5" -> SlidingWindowRateLimiter(it.value.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
                    "acc-23" -> SlidingWindowRateLimiter(it.value.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
                    else -> null
                }
            }
}