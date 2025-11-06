package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Counter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
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
import io.micrometer.core.instrument.Timer
import kotlin.time.DurationUnit
import kotlin.time.measureTime

@Service
class OrderPayer(
    private val meterRegistry: MeterRegistry,
) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        11,
        11,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val acceptedRequestsCounter: Counter = Counter
        .builder("incoming.payments.accepted")
        .register(meterRegistry)

    private val slidingWindowLimiter = SlidingWindowRateLimiter(
        rate = 10,
        window = Duration.ofSeconds(1)
    )

    private fun reject(reason: String, retryAfterMillis: Long): Nothing {
        logger.trace("Rejecting payment due to $reason, retryAfter=${retryAfterMillis}ms")
        Counter.builder("incoming.payments.rejected")
            .tag("reason", reason)
            .register(meterRegistry)
            .increment()
        throw TooManyRequestsException(System.currentTimeMillis() + retryAfterMillis)
    }

    private val requestLatency: Timer = Timer.builder("request_latency")
        .description("Время выполнения запросов к внешней платёжной системе")
        .publishPercentiles(0.5, 0.8, 0.9, 0.99)
        .register(meterRegistry)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        when {
            !slidingWindowLimiter.tick() -> reject("rate_limit", 1000)
            paymentExecutor.queue.remainingCapacity() == 0 -> reject("queue_full", 1000)
            else -> acceptedRequestsCounter.increment()
        }

        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
            val paymentTime = measureTime {
                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
            requestLatency.record(paymentTime.toLong(DurationUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
        }
        return createdAt
    }

    class TooManyRequestsException(val retryAfterMillis: Long) : RuntimeException("Too many incoming requests")
}