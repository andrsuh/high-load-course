package ru.quipy.config

import org.springframework.stereotype.Component
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.util.concurrent.ConcurrentHashMap
import java.time.Duration

@Component
class PaymentRateLimiterFactory {

    private val rateLimiters = ConcurrentHashMap<String, SlidingWindowRateLimiter>()

    fun getRateLimiterForAccount(accountName: String, rateLimitPerSec: Int): SlidingWindowRateLimiter {
        return rateLimiters.computeIfAbsent(accountName) {
            SlidingWindowRateLimiter(
                rate = rateLimitPerSec.toLong(),
                window = Duration.ofSeconds(1)
            )
        }
    }
}