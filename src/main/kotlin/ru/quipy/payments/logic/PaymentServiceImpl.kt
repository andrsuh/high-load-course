package ru.quipy.payments.logic

import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    @PreDestroy
    fun close() {
        paymentAccounts.forEach { it.close() }
    }

    override fun tryReserve(deadline: Long): Boolean {
        val reserved = mutableListOf<PaymentExternalSystemAdapter>()
        for (account in paymentAccounts) {
            if (!account.tryReserve(deadline)) {
                reserved.forEach { it.releaseReservation() }
                return false
            }
            reserved += account
        }
        return true
    }

    override fun releaseReservation() {
        paymentAccounts.forEach { it.releaseReservation() }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }
}