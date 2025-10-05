package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    private val registry: MeterRegistry,
) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val linkedBlockingQueue = LinkedBlockingQueue<Runnable>(16_000)
    private val gauge = Gauge.builder("queue.size", linkedBlockingQueue) { it.size.toDouble() }.register(registry)

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        32,
        60L,
        TimeUnit.SECONDS,
        linkedBlockingQueue,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}