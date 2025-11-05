package ru.quipy.payments.logic

import org.springframework.stereotype.Component
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Component
class PaymentRateLimiterFactory {

    private val rateLimiters = ConcurrentHashMap<String, LeakingBucketRateLimiter>()

    fun getRateLimiterForAccount(accountName: String, rateLimitPerSec: Int): LeakingBucketRateLimiter {
        return rateLimiters.computeIfAbsent(accountName) {
            LeakingBucketRateLimiter(
                rate = 11,
                window = Duration.ofSeconds(1),
                bucketSize = 275
            )
        }
    }
}