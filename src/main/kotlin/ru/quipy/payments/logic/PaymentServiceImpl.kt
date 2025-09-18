package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.FixedWindowRateLimiter
import java.util.*
import java.util.concurrent.TimeUnit


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val rateLimiters = paymentAccounts.associateBy(
        { it.name() },
        { FixedWindowRateLimiter(it.rateLimitPerSec(), 1, TimeUnit.SECONDS) }
    )

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            rateLimiters.getValue(account.name()).tickBlocking()
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }
}