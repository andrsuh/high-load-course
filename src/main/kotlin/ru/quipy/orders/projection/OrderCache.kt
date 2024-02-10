package ru.quipy.orders.projection

import com.github.benmanes.caffeine.cache.Caffeine
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication.Companion.appExecutor
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.*
import ru.quipy.orders.logic.OrderAggregateState
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.time.Duration
import java.util.*
import javax.annotation.PostConstruct

/**
 * Implementation of a eventual-consistent cache of order states
 */
@Service
class OrderCache {

    val logger: Logger = LoggerFactory.getLogger(OrderCache::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var ordersESService: EventSourcingService<UUID, OrderAggregate, OrderAggregateState>

    private val orderCache = Caffeine.newBuilder()
        .maximumSize(5_000)
        .expireAfterWrite(Duration.ofMinutes(5))
        .build<UUID, OrderAggregateState?>()

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(
            OrderAggregate::class,
            "payments:order-cache",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(OrderCreatedEvent::class) { event ->
                    updateOrderState(event.orderId)
            }
            `when`(ItemAddedEvent::class) { event ->
                    updateOrderState(event.orderId)
            }
            `when`(CheckoutCancelledEvent::class) { event ->
                    updateOrderState(event.orderId)
            }
            `when`(OrderCheckoutStartedEvent::class) { event ->
                    updateOrderState(event.orderId)
            }
            `when`(OrderBookingResultSetEvent::class) { event ->
                    updateOrderState(event.orderId)
            }
            `when`(OrderDeliverySetEvent::class) { event ->
                    updateOrderState(event.orderId)
            }
            `when`(OrderPaymentStartedEvent::class) { event ->
                    updateOrderState(event.orderId)
            }
            `when`(OrderPaymentFailedEvent::class) { event ->
                    updateOrderState(event.orderId)
            }
            `when`(OrderPaymentSucceededEvent::class) { event ->
                    updateOrderState(event.orderId)
            }
        }
    }

    fun getOrderState(orderId: UUID): OrderAggregateState? {
        return orderCache.getIfPresent(orderId) ?: run {
            updateOrderStateBlocking(orderId)
            orderCache.getIfPresent(orderId)
        }
    }

    fun updateOrderStateBlocking(orderId: UUID) {
        orderCache.put(orderId, ordersESService.getState(orderId))
    }

    fun updateOrderState(orderId: UUID) {
        appExecutor.submit {
            updateOrderStateBlocking(orderId)
        }
    }
    fun invalidateOrderState(orderId: UUID) {
        orderCache.invalidate(orderId)
    }
}
