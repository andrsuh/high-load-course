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

    val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val (canAccept, expectedCompletionMillis) = paymentService.canAcceptPayment(deadline)
        if (!canAccept) {
            logger.error("429 from OrderPayer")
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                "All payment accounts are under back pressure. Try again later."
            ).also {
                val delaySeconds = (expectedCompletionMillis - System.currentTimeMillis()) / 1000
                it.headers.add("Retry-After", "$delaySeconds")
            }
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