package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimitExceededException
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val queue = LinkedBlockingQueue<Runnable>(8_000)

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        queue,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val rate = 11
    private val waitTime = 13
    // test 2 - 126
    // test 3 - 265
    /*private val bucket = LeakingBucketRateLimiter(
        rate = 8,
        bucketSize = 35,
        window = Duration.ofSeconds(1)
    )*/
    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(8, Duration.ofSeconds(1))
    init {
        Gauge.builder("payment.executor.queue.size") { queue.size.toDouble() }
            .description("Current number of tasks waiting in payment executor queue")
            .tag("component", "order-payer")
            .register(Metrics.globalRegistry)

        Gauge.builder("payment.executor.active.count") { paymentExecutor.activeCount.toDouble() }
            .description("Number of actively executing threads in payment executor")
            .tag("component", "order-payer")
            .register(Metrics.globalRegistry)

        Gauge.builder("payment.executor.completed.count") { paymentExecutor.completedTaskCount.toDouble() }
            .description("Total number of completed tasks by payment executor")
            .tag("component", "order-payer")
            .register(Metrics.globalRegistry)
    }

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val toBlock = deadline - createdAt
        if (!slidingWindowRateLimiter.tickBlocking(Duration.ofMillis(toBlock))) {
            throw RateLimitExceededException()
        }
        //val task = bucket.tick {
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
        //}
        //if (task) return createdAt else throw RateLimitExceededException()
        return createdAt
    }
}