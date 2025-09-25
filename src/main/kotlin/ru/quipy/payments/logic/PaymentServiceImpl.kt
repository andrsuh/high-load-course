package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.parallel.ParallelLimiter
import ru.quipy.common.utils.ratelimiter.RateLimiter
import java.time.Duration
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val rateLimiters: Map<String, RateLimiter?>,
    private val parallelLimiters: Map<String, ParallelLimiter?>,
) : PaymentService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            val rl = rateLimiters[account.name()]
            val pl = parallelLimiters[account.name()]
            val call = { account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline) }

            logger.info("BEFORE_LIMITERS")
            parallelLimit(pl, deadline - Duration.ofSeconds(5).toMillis()) {
                rateLimit(rl, deadline) {
                    call()
                    logger.info("SUCCESS_CALL")
                }
            }
        }
    }

    fun rateLimit(rl: RateLimiter?, deadline: Long, call: () -> Unit) {
        if (rl != null) {
            if (rl.tickBlocking(deadline)) {
                call()
            } else {
                logger.error("Error while performing payment")
            }
        } else {
            call()
        }
    }

    fun<T> parallelLimit(pl: ParallelLimiter?, deadline: Long, call: () -> T): T =
        pl?.queueCallWithTimeout(deadline) { call() } ?: call()
}