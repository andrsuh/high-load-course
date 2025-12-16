package ru.quipy.payments.logic

import org.springframework.stereotype.Component
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

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