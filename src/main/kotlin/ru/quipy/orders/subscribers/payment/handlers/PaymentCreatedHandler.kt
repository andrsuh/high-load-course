package ru.quipy.orders.subscribers.payment.handlers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication
import ru.quipy.common.exceptions.PaymentException
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.logic.PaymentService
import ru.quipy.payments.logic.now
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class PaymentCreatedHandler : EventHandler<PaymentCreatedEvent> {

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var paymentService: PaymentService

    val logger: Logger = LoggerFactory.getLogger(PaymentCreatedHandler::class.java)

    //TODO: kostyl, find either a way to launch coroutines and forget, or any other way that will fix this
    private val threadPool = ThreadPoolExecutor(
        12,
        12,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(),
        NamedThreadFactory("payment-created-handler-executors")
    )


    override fun handle(event: PaymentCreatedEvent) {
        threadPool.submit handle@{
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