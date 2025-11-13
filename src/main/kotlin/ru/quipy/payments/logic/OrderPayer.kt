package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
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
import java.util.concurrent.*
import io.micrometer.core.instrument.Timer
import kotlin.time.DurationUnit
import kotlin.time.measureTime

@Service
class OrderPayer(
    private val meterRegistry: MeterRegistry,
) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

        private const val PARALLEL_HTTP = 50

        private const val CORE_POOL_SIZE = (1 * PARALLEL_HTTP).toInt()
        private const val MAX_WAIT_MS = 200L
        private const val RATE_PER_SEC_LIMIT = 100
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        CORE_POOL_SIZE,
        PARALLEL_HTTP,
        0L, TimeUnit.MILLISECONDS,
        SynchronousQueue<Runnable>(),
        NamedThreadFactory("payment-http-executor"),
        CallerBlockingRejectedExecutionHandler(Duration.ofMillis(MAX_WAIT_MS))
    ).apply { prestartAllCoreThreads() }

    @Suppress("unused")
    private val poolActiveGauge = Gauge.builder("payment.pool.active") { paymentExecutor.activeCount.toDouble() }
        .description("Активные потоки в пуле")
        .register(meterRegistry)

    @Suppress("unused")
    private val poolSizeGauge = Gauge.builder("payment.pool.size") { paymentExecutor.poolSize.toDouble() }
        .description("Текущий размер пула")
        .register(meterRegistry)

    @Suppress("unused")
    private val poolUtilizationGauge = Gauge.builder("payment.pool.utilization") {
        val currentSize = paymentExecutor.poolSize.coerceAtLeast(1)
        paymentExecutor.activeCount.toDouble() / currentSize
    }.description("Доля занятых потоков")
        .register(meterRegistry)

    @Suppress("unused")
    private val poolCompletedGauge = Gauge.builder("payment.pool.completed") { paymentExecutor.completedTaskCount.toDouble() }
        .description("Сколько задач выполнено пулом")
        .register(meterRegistry)

    private val acceptedRequestsCounter: Counter = Counter
        .builder("incoming.payments.accepted")
        .register(meterRegistry)

    private val slidingWindowLimiter = SlidingWindowRateLimiter(
        rate = RATE_PER_SEC_LIMIT.toLong(),
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

        if (!slidingWindowLimiter.tick()) {
            reject("rate_limit", 1000)
        }

        try {
            paymentExecutor.execute {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
                val paymentTime = measureTime {
                    paymentService.submitPaymentRequest(
                        paymentId,
                        amount,
                        createdAt,
                        deadline)
                }
                requestLatency.record(paymentTime.toLong(DurationUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
            }
            acceptedRequestsCounter.increment()
        } catch (e: RejectedExecutionException) {
            reject("queue_full", 1000)
        }

        return createdAt
    }

    class TooManyRequestsException(val retryAfterMillis: Long) : RuntimeException("Too many incoming requests")

    @jakarta.annotation.PreDestroy
    fun shutdown() {
        paymentExecutor.shutdown()
        if (!paymentExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            paymentExecutor.shutdownNow()
        }
    }
}