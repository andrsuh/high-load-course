package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
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

    private val paymentExecutor = ThreadPoolExecutor(
        256,
        256,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(64),
        NamedThreadFactory("payment-submission-executor"),
        RejectedExecutionHandler { _, _ ->
            logger.warn("Payment rejected: payment queue is full")
            throw RejectedExecutionException("Payment queue is full")
        }
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val acceptedAt = System.currentTimeMillis()
        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            // The external budget starts when the worker is ready to submit,
            // not when the request was waiting in the local executor queue.
            val submissionStartedAt = System.currentTimeMillis()
            paymentService.submitPaymentRequest(paymentId, amount, submissionStartedAt, deadline)
        }
        return acceptedAt
    }
}