package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.beans.factory.annotation.Value
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import ru.quipy.common.utils.PaymentPriorityQueue
import ru.quipy.common.utils.PaymentTask
import java.util.concurrent.atomic.AtomicLong
import jakarta.annotation.PreDestroy
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.RejectedExecutionException

@Service
class OrderPayer(
    private val meterRegistry: io.micrometer.core.instrument.MeterRegistry,
    private val paymentService: PaymentService,
    @Value("\${payment.queue.capacity:110}") queueCapacity: Int,
    @Value("\${payment.admission.max-in-flight:300}") maxInFlight: Int,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    init { require(maxInFlight > 0) }

    private val workers = paymentService.workerCount()

    private val admission = ru.quipy.common.utils.PaymentAdmissionLimiter(
        paymentService.admissionRate(), maxInFlight = maxInFlight,
        windowMillis = TimeUnit.SECONDS.toMillis(1)
    )
    private val sequence = AtomicLong()
    private val paymentExecutor = ThreadPoolExecutor(
        workers,
        workers,
        30L,
        TimeUnit.SECONDS,
        PaymentPriorityQueue(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    private val dispatchScheduler = ru.quipy.common.utils.PaymentAdmissionDispatcher(admission, paymentExecutor) { e ->
        meterRegistry.counter("payment.admission.rejected").increment()
        logger.warn("Scheduled payment dispatch rejected", e)
    }

    init {
        paymentExecutor.prestartAllCoreThreads()
        io.micrometer.core.instrument.Gauge.builder("payment.admission.scheduled", dispatchScheduler) {
            it.pendingCount.toDouble()
        }.register(meterRegistry)
        io.micrometer.core.instrument.Gauge.builder("payment.queue.size", paymentExecutor) {
            it.queue.size.toDouble()
        }.register(meterRegistry)
        io.micrometer.core.instrument.Gauge.builder("payment.workers.active", paymentExecutor) {
            it.activeCount.toDouble()
        }.register(meterRegistry)
    }

    @PreDestroy
    fun shutdown() {
        dispatchScheduler.close()
        paymentExecutor.shutdown()
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val providerMean = deadline - paymentService.latestStart(deadline)
        val waitBudget = deadline - createdAt - 2 * providerMean - 200
        val dispatchDelayAllowance = waitBudget.coerceAtLeast(0) / 20
        val maxWait = waitBudget - dispatchDelayAllowance
        val task = PaymentTask(amount, paymentService.latestStart(deadline), sequence.incrementAndGet()) {
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
            } catch (e: Exception) {
                meterRegistry.counter("payment.dispatch.errors").increment()
                logger.error("Payment task failed for payment $paymentId", e)
            }
        }
        if (!dispatchScheduler.submit(maxWait, 2 * providerMean, task)) {
            meterRegistry.counter("payment.admission.limited").increment()
            throw RejectedExecutionException("Payment cannot be dispatched within its deadline")
        }
        return createdAt
    }
}
