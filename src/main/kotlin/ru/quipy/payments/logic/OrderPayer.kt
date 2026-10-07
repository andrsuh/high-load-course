package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.*

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    private val meterRegistry: MeterRegistry,
) {
    companion object {
        val logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    private inner class ScheduledPayment(
        val orderId: UUID,
        val amount: Int,
        val paymentId: UUID,
        val deadline: Long,
        val createdAt: Long
    ) : Runnable {
        override fun run() {
            val now = System.currentTimeMillis()
            val waitTimeMs = now - createdAt
            queueWaitTimer.record(waitTimeMs, TimeUnit.MILLISECONDS)

            val createdEvent = paymentESService.create {
                it.create(paymentId, orderId, amount)
            }

            if (now >= deadline) {
                logger.warn(
                    "Payment $paymentId for order $orderId REJECTED: " +
                        "deadline exceeded by ${now - deadline}ms"
                )
                paymentESService.update(paymentId) {
                    it.logSubmission(
                        success = false,
                        UUID.randomUUID(),
                        now,
                        Duration.ofMillis(waitTimeMs)
                    )
                    it.logProcessing(
                        false,
                        now,
                        UUID.randomUUID(),
                        reason = "Deadline exceeded before processing (waited ${waitTimeMs}ms)"
                    )
                }
                return
            }

            val timeRemaining = deadline - now
            if (timeRemaining < 1_000) {
                logger.warn(
                    "Payment $paymentId for order $orderId has only ${timeRemaining}ms left, skipping"
                )
                paymentESService.update(paymentId) {
                    it.logSubmission(
                        success = false,
                        UUID.randomUUID(),
                        now,
                        Duration.ofMillis(waitTimeMs)
                    )
                    it.logProcessing(
                        false,
                        now,
                        UUID.randomUUID(),
                        reason = "Insufficient time remaining: ${timeRemaining}ms"
                    )
                }
                return
            }

            logger.trace(
                "Payment ${createdEvent.paymentId} for order $orderId created " +
                    "with ${timeRemaining}ms remaining"
            )
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
    }

    private val paymentQueue = PriorityBlockingQueue<Runnable>(
        8_000,
        Comparator { left, right ->
            (left as ScheduledPayment).deadline.compareTo((right as ScheduledPayment).deadline)
        }
    )

    private val paymentExecutor = ThreadPoolExecutor(
        8,
        8,
        0L,
        TimeUnit.MILLISECONDS,
        paymentQueue,
        ThreadFactory { runnable ->
            Thread(runnable, "payment-priority-executor").apply {
                isDaemon = true
                priority = Thread.NORM_PRIORITY
            }
        },
        ThreadPoolExecutor.CallerRunsPolicy()
    )
    private val queueWaitTimer = Timer.builder("payment_queue_wait_time")
        .description("Time a payment waits in the priority queue")
        .publishPercentileHistogram()
        .register(meterRegistry)

    init {
        Gauge.builder("payment_queue_size") { paymentQueue.size.toDouble() }
            .description("Current number of tasks waiting in the payment priority queue")
            .register(meterRegistry)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        checkCanAccept(deadline)

        val scheduledPayment = ScheduledPayment(orderId, amount, paymentId, deadline, createdAt)

        paymentExecutor.execute(scheduledPayment)

        return createdAt
    }

    fun checkCanAccept(deadline: Long) {
        if (!paymentService.canAccept(deadline, paymentQueue.size)) {
            throw org.springframework.web.server.ResponseStatusException(
                org.springframework.http.HttpStatus.TOO_MANY_REQUESTS,
                "Payment cannot complete before deadline"
            )
        }
    }

    fun shutdown() {
        paymentExecutor.shutdown()
        if (!paymentExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
            paymentExecutor.shutdownNow()
        }
    }
}