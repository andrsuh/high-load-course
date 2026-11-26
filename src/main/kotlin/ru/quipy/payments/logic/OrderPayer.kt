package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    meterRegistry: MeterRegistry,
    @Value("\${payment.service-name}") serviceName: String,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

        private const val QUEUE_CAPACITY = 1_000
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val rejectedPaymentsCounter = meterRegistry.counter("payment.admission.rejected", "service", serviceName)

    private val paymentExecutor = ThreadPoolExecutor(
        128,
        128,
        0L,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(QUEUE_CAPACITY),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy(),
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        if (!paymentService.tryReserve(deadline)) {
            rejectedPaymentsCounter.increment()
            throw RejectedExecutionException("Payment $paymentId can't be processed before deadline")
        }

        try {
            submit(orderId, amount, paymentId, createdAt, deadline)
        } catch (e: Exception) {
            paymentService.releaseReservation()
            if (e is RejectedExecutionException) rejectedPaymentsCounter.increment()
            throw e
        }
        return createdAt
    }

    private fun submit(orderId: UUID, amount: Int, paymentId: UUID, createdAt: Long, deadline: Long) {
        val createdEvent = paymentESService.create {
            it.create(
                paymentId,
                orderId,
                amount
            )
        }
        logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

        paymentExecutor.execute {
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
    }
}
