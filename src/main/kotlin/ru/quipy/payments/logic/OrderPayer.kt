package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.exceptions.DeadlineExceededException
import ru.quipy.exceptions.TooManyRequestsException
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.dto.Transaction
import java.time.Duration
import java.util.*
import java.util.concurrent.*

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    private val accountProperties: PaymentAccountProperties,
    @field:Qualifier("parallelLimiter")
    private val parallelLimiter: Semaphore,
) {
    private val paymentProcessingPlannedCounter: Counter =
        Metrics.counter("payment.processing.planned", "accountName", accountProperties.accountName)
    private val paymentProcessingStartedCounter: Counter =
        Metrics.counter("payment.processing.started", "accountName", accountProperties.accountName)
    private val paymentProcessingCompletedCounter: Counter =
        Metrics.counter("payment.processing.completed", "accountName", accountProperties.accountName)

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    private val paymentExecutor: ThreadPoolExecutor by lazy {
        ThreadPoolExecutor(
            11,
            11,
            0L,
            TimeUnit.MILLISECONDS,
            ArrayBlockingQueue<Runnable>(250),
            NamedThreadFactory("payment-submission-executor"),
            ThreadPoolExecutor.AbortPolicy()
        )
    }

    private val slidingWindowRateLimiter: SlidingWindowRateLimiter by lazy {
        SlidingWindowRateLimiter(
            rate = accountProperties.rateLimitPerSec.toLong(),
            window = Duration.ofSeconds(1),
        )
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val now = System.currentTimeMillis()
        if (now >= deadline) throw DeadlineExceededException()

        paymentProcessingPlannedCounter.increment()

        if (!parallelLimiter.tryAcquire(5, TimeUnit.MILLISECONDS)) {
            throw TooManyRequestsException(3)
        }

        try {
            while (!slidingWindowRateLimiter.tick()) {
                if (System.currentTimeMillis() >= deadline) {
                    throw DeadlineExceededException()
                }
                Thread.sleep(10)
            }

            paymentProcessingStartedCounter.increment()

            val event = paymentESService.create { it.create(paymentId, orderId, amount) }
            paymentService.submitPaymentRequest(paymentId, amount, now, deadline)

            return now
        } finally {
            parallelLimiter.release()
            paymentProcessingCompletedCounter.increment()
        }
    }
}