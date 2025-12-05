package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.atomic.AtomicInteger


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, activeRequestsCount: AtomicInteger) {
        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, activeRequestsCount)
        }
    }

    override fun getTotalOptimalThreads(): Int {
        return paymentAccounts
            .filter { it.isEnabled() }
            .sumOf { it.getOptimalThreads() }
    }
}