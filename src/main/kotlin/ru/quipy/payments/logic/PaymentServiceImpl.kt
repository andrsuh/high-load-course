package ru.quipy.payments.logic

import io.github.resilience4j.ratelimiter.RateLimiter
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    rateLimiters: List<RateLimiter>,
) : PaymentService {
    private val rateLimitersByName = rateLimiters.associateBy { it.name }

    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val slidingWindow = SlidingWindowRateLimiter(10, Duration.ofSeconds(1))

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            val call = { account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline) }

            runCatching {
                slidingWindow.tickBlocking()
                call()
                return@runCatching
                // rateLimitersByName[account.name()]?.executeCallable(1000, call) ?: call()
            }.onFailure {
                logger.error("Error while performing payment", it)
            }
        }
    }
}