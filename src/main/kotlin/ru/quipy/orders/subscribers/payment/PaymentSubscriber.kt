package ru.quipy.orders.subscribers.payment

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication
import ru.quipy.apigateway.APIController
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.orders.subscribers.payment.handlers.EventHandler
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.time.Duration
import javax.annotation.PostConstruct

@Service
class PaymentSubscriber {

    @Autowired
    private lateinit var orderRepository: OrderRepository
    val logger: Logger = LoggerFactory.getLogger(PaymentSubscriber::class.java)


    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var paymentCreatedEventHandler: EventHandler<PaymentCreatedEvent>


    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(
            PaymentAggregate::class,
            "orders:payment-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(PaymentProcessedEvent::class) { event ->
                OnlineShopApplication.Companion.appExecutor.submit {
//                    orderRepository.findById(event.orderId)?.let {
//                        orderRepository.save(
//                            it.copy(
//                                paymentHistory = (it.paymentHistory + PaymentTransactionsSubscriber.PaymentLogRecord(
//                                    event.processedAt,
//                                    if (event.success) PaymentTransactionsSubscriber.PaymentStatus.SUCCESS else PaymentTransactionsSubscriber.PaymentStatus.FAILED,
//                                    event.amount,
//                                    event.paymentId
//                                ))
//                            )
//                        )
//                    } ?: IllegalStateException("Order with id ${event.orderId} not found")

                    logger.info(
                        "Payment results. OrderId ${event.orderId}, succeeded: ${event.success}, txId: ${event.transactionId}, reason: ${event.reason}, duration: ${
                            Duration.ofMillis(
                                event.createdAt - event.submittedAt
                            ).toSeconds()
                        }, spent in queue: ${event.spentInQueueDuration.toSeconds()}"
                    )
                }
            }
        }

        subscriptionsManager.createSubscriber(
            PaymentAggregate::class,
            "orders:payment-processor-subscriber",
            retryConf = RetryConf(3, RetryFailedStrategy.SKIP_EVENT)
        )
        {
            `when`(PaymentCreatedEvent::class) {
                event -> paymentCreatedEventHandler.handle(event)
            }
        }

    }
}