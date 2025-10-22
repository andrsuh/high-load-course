package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

//    private var rateLimit = LeakingBucketQueueRateLimiter(1L, 91.milliseconds, 160)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val (canAccept, estimatedWaitMs) = paymentService.canAcceptPayment(deadline)
        if (!canAccept) {
            logger.error("429 from OrderPayer")
            val delaySeconds = (estimatedWaitMs - System.currentTimeMillis()) / 1000
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                delaySeconds.toString()
            )
        }


        val createdAt = System.currentTimeMillis()
        val createdEvent = paymentESService.create {
            it.create(
                paymentId,
                orderId,
                amount
            )
        }
        logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)

        paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        return createdAt
    }
}