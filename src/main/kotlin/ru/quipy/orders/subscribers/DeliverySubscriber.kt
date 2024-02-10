package ru.quipy.orders.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication.Companion.appExecutor
import ru.quipy.core.EventSourcingService
import ru.quipy.delivery.api.DeliveryAggregate
import ru.quipy.delivery.api.DeliveryCreatedEvent
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.logic.OrderAggregateState
import ru.quipy.orders.logic.setDelivery
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.*
import javax.annotation.PostConstruct

@Service
class DeliverySubscriber {

    val logger: Logger = LoggerFactory.getLogger(DeliverySubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var ordersESService: EventSourcingService<UUID, OrderAggregate, OrderAggregateState>

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(
            DeliveryAggregate::class,
            "orders:delivery-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(DeliveryCreatedEvent::class) { event ->
                appExecutor.submit {
                    logger.info("Started setting delivery ${event.deliveryId} for order ${event.orderId}")
                    ordersESService.update(event.orderId) {
                        it.setDelivery(event.deliveryId)
                    }
                    logger.info("Delivery ${event.deliveryId} set for order ${event.orderId}")
                }
            }
        }
    }
}
