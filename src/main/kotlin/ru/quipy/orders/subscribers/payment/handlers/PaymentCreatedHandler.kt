package ru.quipy.orders.subscribers.payment.handlers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Import
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication
import ru.quipy.common.exceptions.PaymentException
import ru.quipy.common.utils.RateLimiter
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.orders.subscribers.payment.config.PaymentProcessingHandlerConfiguration
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.logic.PaymentService
import ru.quipy.payments.logic.now

@Service
@Import(PaymentProcessingHandlerConfiguration::class)
class PaymentCreatedHandler : EventHandler<PaymentCreatedEvent> {

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var paymentService: PaymentService

    val logger: Logger = LoggerFactory.getLogger(PaymentCreatedHandler::class.java)


    override fun handle(event: PaymentCreatedEvent) {
        OnlineShopApplication.Companion.appExecutor.submit handle@{
            val order = orderRepository.findById(event.orderId)

            if (order == null) {
                logger.error("Order ${event.orderId} was not found.")

                PaymentException.paymentFailure("Order ${event.orderId} was not found.")
                return@handle
            }

            paymentService.submitPaymentRequest(event.paymentId, event.amount, now(), event.deadline)
        }
    }
}