package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    override fun parallelism() = paymentAccounts.sumOf { it.parallelism() }

    // Оплата уходит во все аккаунты, поэтому отправлять её можно, когда подошёл слот у каждого
    override fun reserveSendSlot(deadline: Long): Long? {
        val slots = paymentAccounts.map { it.reserveSendSlot(deadline) ?: return null }
        return slots.max()
    }

    override fun retryAfter(deadline: Long) = paymentAccounts.maxOf { it.retryAfter(deadline) }
}