package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
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

@Service
class OrderPayer(
    private val meterRegistry: io.micrometer.core.instrument.MeterRegistry,
    private val paymentService: PaymentService,
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
        PaymentPriorityQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    init {
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
        paymentExecutor.execute(PaymentTask(amount, paymentService.latestStart(deadline), sequence.incrementAndGet()) {
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
        })
        return createdAt
    }
}
