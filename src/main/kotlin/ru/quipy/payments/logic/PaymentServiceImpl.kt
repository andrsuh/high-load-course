package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    override fun workerCount(): Int = paymentAccounts.minOf { it.parallelCapacity() }.coerceAtLeast(1)

    override fun latestStart(deadline: Long): Long = paymentAccounts.minOf { it.latestStart(deadline) }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }
}
