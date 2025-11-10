package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.config.PaymentMetrics
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    private val paymentMetrics: PaymentMetrics,
    @Value("\${payment.active.limit:256}") private val maxConcurrentRequests: Int,
    @Value("\${payment.accounts:acc-23}") private val accountsProperty: String,
) {

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    private val activeRequests = AtomicInteger(0)

    private val accountLabels = accountsProperty
        .split(",")
        .map { it.trim() }
        .filter { it.isNotBlank() }
        .ifEmpty { listOf("default") }

    @PostConstruct
    fun init() {
        accountLabels.forEach { account ->
            paymentMetrics.registerActiveRequestsGauge(account) { activeRequests.get() }
        }
    }

    fun canAcceptRequest(): Boolean {
        return activeRequests.get() < maxConcurrentRequests
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (createdAt >= deadline) {
            logger.debug("Payment request skipped: deadline already expired for $paymentId")
            return createdAt
        }

        paymentESService.create {
            it.create(paymentId, orderId, amount)
        }

        paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline, activeRequests)

        return createdAt
    }

    fun getQueueSize(): Int {
        return activeRequests.get()
    }
}
