package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.time.Duration


@Service
class OrderPayer(meterRegistry: MeterRegistry, @Value("\${payment.rps:16}") private val rateLimitPerSec: Int,) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    private val paymentLimiter =
        LeakingBucketRateLimiter(
            rateLimitPerSec.toLong(),
            Duration.ofSeconds(1),
            rateLimitPerSec * 3,
        )
    private val processPaymentsGauge = AtomicInteger()

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(7),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    init {
        meterRegistry.gauge("process_payments_gauge", processPaymentsGauge)
    }


    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (deadline <= createdAt) {
            throw TooManyRequestsException(createdAt + 100)
        }

        val deadlineTimeout = maxOf(0, deadline - createdAt)
        if (!paymentLimiter.tickBlocking(Duration.ofMillis(deadlineTimeout))) {
            throw TooManyRequestsException(createdAt + 100)
        }

        if (paymentExecutor.queue.remainingCapacity() == 0) {
            throw TooManyRequestsException(createdAt + 100)
        }

        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        processPaymentsGauge.set(paymentExecutor.queue.size)
        return createdAt
    }
}