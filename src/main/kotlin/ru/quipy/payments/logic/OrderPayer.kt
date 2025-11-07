package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val activeRequestsCount = AtomicInteger(0)

    private var maxConcurrentRequests = 180

    @PostConstruct
    fun init() {
        maxConcurrentRequests = calculateMaxConcurrentRequests()
    }

    private fun calculateMaxConcurrentRequests(): Int {
        val totalThreads = paymentService.getTotalOptimalThreads()
        return if (totalThreads > 0) {
            (totalThreads * 1.2).toInt()
        } else {
            180
        }
    }

    fun canAcceptRequest(): Boolean {
        return activeRequestsCount.get() < maxConcurrentRequests
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (createdAt >= deadline) {
            return createdAt
        }

        paymentESService.create {
            it.create(paymentId, orderId, amount)
        }

        paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline, activeRequestsCount)

        return createdAt
    }

    fun getQueueSize(): Int {
        return activeRequestsCount.get()
    }
}