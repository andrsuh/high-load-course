package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.apigateway.TooManyRequestsException
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.config.PaymentMetrics
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    private val paymentMetrics: PaymentMetrics,
    @Value("\${payment.buffer.capacity:2048}") private val bufferCapacity: Int,
    @Value("\${payment.buffer.dispatcher-threads:32}") private val dispatcherThreads: Int,
    @Value("\${payment.accounts:acc-23}") private val accountsProperty: String,
) {

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    private val submissionQueue = LinkedBlockingQueue<Runnable>(bufferCapacity)
    private val dispatcherExecutor = ThreadPoolExecutor(
        dispatcherThreads,
        dispatcherThreads,
        0L,
        TimeUnit.MILLISECONDS,
        submissionQueue,
        NamedThreadFactory("payment-buffer")
    )

    private val queueAccountLabels = accountsProperty
        .split(",")
        .map { it.trim() }
        .filter { it.isNotBlank() }
        .ifEmpty { listOf("default") }

    @PostConstruct
    fun init() {
        queueAccountLabels.forEach { account ->
            paymentMetrics.registerBufferQueueGauge(account) { submissionQueue.size }
        }
    }

    fun canAcceptRequest(): Boolean {
        return submissionQueue.remainingCapacity() > 0
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (createdAt >= deadline) {
            return createdAt
        }

        val task = Runnable {
            paymentESService.create {
                it.create(paymentId, orderId, amount)
            }
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        try {
            dispatcherExecutor.execute(task)
        } catch (rex: RejectedExecutionException) {
            queueAccountLabels.forEach { account ->
                paymentMetrics.incrementRejected(account, "buffer_full")
            }
            val queueSize = submissionQueue.size
            logger.warn("Payment buffer overflow. queueSize={}, capacity={}", queueSize, bufferCapacity)
            throw TooManyRequestsException("System overloaded. Current queue: $queueSize")
        }

        return createdAt
    }

    fun getQueueSize(): Int {
        return submissionQueue.size
    }
}
