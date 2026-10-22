package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
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
import java.util.concurrent.atomic.AtomicInteger

@Service
class OrderPayer(meterRegistry: MeterRegistry) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        330,
        330,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val paymentsInProgress = AtomicInteger()

    private val acceptedPayments = Counter.builder("payment.incoming").tag("result", "accepted").register(meterRegistry)
    private val rejectedPayments = Counter.builder("payment.incoming").tag("result", "rejected").register(meterRegistry)

    init {
        Gauge.builder("payment.inprogress", paymentsInProgress) { it.get().toDouble() }.register(meterRegistry)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        val estimatedFinish = paymentService.estimatedFinishTime(paymentsInProgress.getAndIncrement())
        if (estimatedFinish > deadline) {
            paymentsInProgress.decrementAndGet()
            rejectedPayments.increment()
            throw TooManyRequestsException(retryAfter = createdAt + (estimatedFinish - deadline))
        }
        acceptedPayments.increment()

        paymentExecutor.submit {
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } finally {
                paymentsInProgress.decrementAndGet()
            }
        }
        return createdAt
    }
}

/**
 * Payment can't be processed before its deadline. [retryAfter] is epoch millis when it makes sense to retry.
 */
class TooManyRequestsException(val retryAfter: Long) : RuntimeException("Payment can't be processed in time")
