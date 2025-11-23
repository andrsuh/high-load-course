package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.*

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

    private val linkedBlockingQueue = LinkedBlockingQueue<Runnable>(10_000)
    private val gauge = Gauge.builder("queue.size", linkedBlockingQueue) { it.size.toDouble() }.register(registry)

    private val paymentExecutor = ThreadPoolExecutor(
        250,
        1000,
        50L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>(linkedBlockingQueue),
        NamedThreadFactory("payment-submission-executor"),
        RejectedExecutionHandler { _, _ -> throw TooManyException(System.currentTimeMillis() + 300)
        }
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (deadline <= createdAt) {
            throw TooManyException(createdAt + 500)
        }

        if (paymentExecutor.queue.remainingCapacity() <= 2) {
            throw TooManyException(createdAt + 300)
        }

        try {
            paymentExecutor.submit {
                try {
                    val createdEvent = paymentESService.create { it.create(paymentId, orderId, amount) }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                } catch (e: Exception) {
                    logger.error("Error processing payment $paymentId for order $orderId", e)
                }
            }
        } catch (e: RejectedExecutionException) {
            throw TooManyException(System.currentTimeMillis() + 400)
        }
        return createdAt
    }
}

class TooManyException(val retryAfterMillis: Long) : RuntimeException()