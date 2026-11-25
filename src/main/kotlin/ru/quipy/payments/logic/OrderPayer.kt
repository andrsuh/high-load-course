package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val INPUT_RATE_LIMIT_PER_SEC = 11L
        private const val QUEUE_CAPACITY = 1
        private const val WORKER_COUNT = 110
        private const val RETRY_AFTER_DELAY_MS = 500L
    }

    private class PaymentTask(val deadline: Long, private val action: () -> Unit) : Runnable, Comparable<PaymentTask> {
        override fun run() = action()
        override fun compareTo(other: PaymentTask): Int = deadline.compareTo(other.deadline)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val inputRateLimiter = SlidingWindowRateLimiter(INPUT_RATE_LIMIT_PER_SEC, Duration.ofSeconds(1))

    private val queue = PriorityBlockingQueue<Runnable>(QUEUE_CAPACITY)

    private val paymentExecutor = ThreadPoolExecutor(
        WORKER_COUNT,
        WORKER_COUNT,
        0L,
        TimeUnit.MILLISECONDS,
        queue,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    @Synchronized
    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        if (queue.size >= QUEUE_CAPACITY) {
            throw PaymentQueueFullException(
                retryAfter = System.currentTimeMillis() + RETRY_AFTER_DELAY_MS
            )
        }
        if (!inputRateLimiter.tick()) {
            throw PaymentQueueFullException(
                retryAfter = System.currentTimeMillis() + RETRY_AFTER_DELAY_MS
            )
        }
        val createdAt = System.currentTimeMillis()
        paymentExecutor.execute(PaymentTask(deadline) {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        })
        return createdAt
    }
}
