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
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val sequence = AtomicLong()
    private val paymentExecutor = ThreadPoolExecutor(
        paymentService.workerCount(),
        paymentService.workerCount(),
        0L,
        TimeUnit.MILLISECONDS,
        PaymentPriorityQueue(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    init {
        paymentExecutor.prestartAllCoreThreads()
        io.micrometer.core.instrument.Gauge.builder("payment.queue.size", paymentExecutor) {
            it.queue.size.toDouble()
        }.register(meterRegistry)
        io.micrometer.core.instrument.Gauge.builder("payment.workers.active", paymentExecutor) {
            it.activeCount.toDouble()
        }.register(meterRegistry)
    }

    @PreDestroy
    fun shutdown() {
        paymentExecutor.shutdown()
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
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
        try {
            paymentExecutor.execute(task)
        } catch (e: RejectedExecutionException) {
            meterRegistry.counter("payment.admission.rejected").increment()
            throw e
        }
        return createdAt
    }
}
