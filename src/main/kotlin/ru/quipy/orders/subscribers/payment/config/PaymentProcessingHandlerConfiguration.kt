package ru.quipy.orders.subscribers.payment.config

import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration

@Configuration
class PaymentProcessingHandlerConfiguration {

    val rateLimitConfig = RateLimitConfig(
        rate = 9,
        window = Duration.ofSeconds(1),
    )

    val rateLimiter: RateLimiter = SlidingWindowRateLimiter(rateLimitConfig.rate, rateLimitConfig.window)
}