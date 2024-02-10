package ru.quipy.orders.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication.Companion.appExecutor
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.logic.OrderAggregateState
import ru.quipy.orders.logic.setPaymentResults
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.time.Duration
import java.util.*
import javax.annotation.PostConstruct

@Service
class PaymentSubscriber {

    val logger: Logger = LoggerFactory.getLogger(PaymentSubscriber::class.java)

    @Autowired
    private lateinit var ordersESService: EventSourcingService<UUID, OrderAggregate, OrderAggregateState>

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(PaymentAggregate::class, "orders:payment-subscriber", retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)) {
            `when`(PaymentProcessedEvent::class) { event ->
                appExecutor.submit {
                    ordersESService.update(event.orderId) {
                        it.setPaymentResults(event.paymentId, event.success, event.transactionId, event.reason)
                    }
                    logger.info("Payment results. OrderId ${event.orderId}, succeeded: ${event.success}, txId: ${event.transactionId}, reason: ${event.reason}, duration: ${Duration.ofMillis(event.createdAt - event.submittedAt).toSeconds()}, spent in queue: ${event.spentInQueueDuration.toSeconds()}")
                }
            }
        }
    }
}