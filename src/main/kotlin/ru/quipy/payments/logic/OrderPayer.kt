package ru.quipy.payments.logic

import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
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
        if (!paymentService.canAcceptPayment(deadline)) {
            throw HttpException("Payment service can't accept a new payment", HttpStatus.TOO_MANY_REQUESTS)
        }

        val createdAt = System.currentTimeMillis()
        val createdEvent = paymentESService.create {
            it.create(
                paymentId,
                orderId,
                amount
            )
        }
        logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

        paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        return createdAt
    }
}