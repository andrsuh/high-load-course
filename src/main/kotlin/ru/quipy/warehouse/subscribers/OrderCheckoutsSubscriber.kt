package ru.quipy.warehouse.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication.Companion.appExecutor
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.CheckoutCancelledEvent
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.api.OrderCheckoutStartedEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import ru.quipy.warehouse.api.BookingAggregate
import ru.quipy.warehouse.logic.*
import java.util.*
import javax.annotation.PostConstruct

@Service
class OrderCheckoutsSubscriber {

    val logger: Logger = LoggerFactory.getLogger(OrderCheckoutsSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var bookingESService: EventSourcingService<UUID, BookingAggregate, BookingAggregateState>

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(
            OrderAggregate::class,
            "wh-order-checkouts-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {

            `when`(OrderCheckoutStartedEvent::class) { event ->
                appExecutor.submit {
                    val createdEvent = bookingESService.create {
                        it.create(
                            event.bookingId,
                            event.orderId,
                            products = event.items.map { BookProductRequest(it.productId, it.amount) })
                    }
                    logger.info("Order ${event.orderId} booking created: ${createdEvent.bookingId}")
                }
            }

            `when`(CheckoutCancelledEvent::class) { event ->
                appExecutor.submit {
                    val createdEvent = bookingESService.update(event.bookingId) {
                        it.unbook()
                    }
                    logger.warn("Order ${event.orderId} booking ubooked: ${createdEvent.bookingId}")
                }
            }
        }
    }
}