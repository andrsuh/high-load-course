package ru.quipy.orders.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication.Companion.appExecutor
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.logic.OrderAggregateState
import ru.quipy.orders.logic.setBookingResults
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import ru.quipy.warehouse.api.BookingAggregate
import ru.quipy.warehouse.api.BookingFailedEvent
import ru.quipy.warehouse.api.BookingSucceededEvent
import java.util.*
import javax.annotation.PostConstruct

@Service
class OrdersBookingsSubscriber {

    val logger: Logger = LoggerFactory.getLogger(OrdersBookingsSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var ordersESService: EventSourcingService<UUID, OrderAggregate, OrderAggregateState>

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(BookingAggregate::class, "orders:bookings-subscriber", retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)) {
            `when`(BookingSucceededEvent::class) { event ->
                appExecutor.submit {
                    logger.info("[Order service] Booking success for ${event.orderId}")
                    ordersESService.update(event.orderId) {
                        it.setBookingResults(event.bookingId, true, event.succeededProducts.sumOf { it.amount * it.price })
                    }
                }
            }

            `when`(BookingFailedEvent::class) { event ->
                appExecutor.submit {
                    logger.info("[Order service] Booking failed for ${event.orderId}")
                    ordersESService.update(event.orderId) {
                        it.setBookingResults(event.bookingId, false)
                    }
                }
            }
        }
    }
}
