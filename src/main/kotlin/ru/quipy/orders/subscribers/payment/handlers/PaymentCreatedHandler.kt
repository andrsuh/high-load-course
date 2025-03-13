package ru.quipy.orders.subscribers.payment.handlers

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication
import ru.quipy.common.exceptions.PaymentException
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.logic.PaymentService
import ru.quipy.payments.logic.now

@Service
class PaymentCreatedHandler : EventHandler<PaymentCreatedEvent> {

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var paymentService: PaymentService

    //TODO: to config
    private val semaphore: Semaphore = Semaphore(14, 0)

    val logger: Logger = LoggerFactory.getLogger(PaymentCreatedHandler::class.java)

    override suspend fun handle(event: PaymentCreatedEvent) {
        semaphore.acquire()

        OnlineShopApplication.Companion.appExecutor.submit {
            runBlocking {
                try {
                    val order = orderRepository.findById(event.orderId)

                    if (order == null) {
                        logger.error("Order ${event.orderId} was not found.")

                        PaymentException.paymentFailure("Order ${event.orderId} was not found.")

                    }
                    paymentService.submitPaymentRequest(event.paymentId, event.amount, now(), event.deadline)
                }
                finally {
                    semaphore.release()
                }
            }
        }
    }
}