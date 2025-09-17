package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.ratelimiter.RateLimiter
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val rateLimiters: Map<String, RateLimiter?>,
) : PaymentService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            val rl = rateLimiters[account.name()]
            val call = { account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline) }

            if (rl != null) {
                if (rl.tickBlocking(deadline)) {
                    call()
                } else {
                    logger.error("Error while performing payment: $paymentId")
                }
            } else {
                call()
            }
        }
    }
}