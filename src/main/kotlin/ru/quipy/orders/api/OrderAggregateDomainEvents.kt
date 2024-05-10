package ru.quipy.orders.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import ru.quipy.orders.logic.CartItem
import java.util.*

const val ORDER_CREATED_EVENT = "ORDER_CREATED_EVENT"
const val ITEM_ADDED_EVENT = "ITEM_ADDED_EVENT"
const val CHECKOUT_CANCELLED_EVENT = "CHECKOUT_CANCELLED_EVENT"
const val CHECKOUT_EVENT = "CHECKOUT_EVENT"
const val ORDER_BOOK_RESULTS_SET_EVENT = "ORDER_BOOK_RESULTS_SET_EVENT"
const val ORDER_DELIVERY_SET_EVENT = "ORDER_DELIVERY_SET_EVENT"
const val ORDER_PAYMENT_STARTED_EVENT = "ORDER_PAYMENT_STARTED_EVENT"
const val ORDER_PAYMENT_FAILED_EVENT = "ORDER_PAYMENT_FAILED_EVENT"
const val ORDER_PAYMENT_SUCCEEDED_EVENT = "ORDER_PAYMENT_SUCCEEDED_EVENT"

@DomainEvent(name = ORDER_CREATED_EVENT)
class OrderCreatedEvent(
    val orderId: UUID,
    val userId: UUID,
    createdAt: Long = System.currentTimeMillis(),
    val amount: Int?,
) : Event<OrderAggregate>(
    name = ORDER_CREATED_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = ITEM_ADDED_EVENT)
class ItemAddedEvent(
    val orderId: UUID,
    val item: CartItem,
    createdAt: Long = System.currentTimeMillis(),
) : Event<OrderAggregate>(
    name = ITEM_ADDED_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = CHECKOUT_CANCELLED_EVENT)
class CheckoutCancelledEvent(
    val orderId: UUID,
    val bookingId: UUID,
    createdAt: Long = System.currentTimeMillis(),
) : Event<OrderAggregate>(
    name = CHECKOUT_CANCELLED_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = CHECKOUT_EVENT)
class OrderCheckoutStartedEvent(
    val orderId: UUID,
    val bookingId: UUID,
    val items: List<CartItem>,
    createdAt: Long = System.currentTimeMillis(),
) : Event<OrderAggregate>(
    name = CHECKOUT_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = ORDER_BOOK_RESULTS_SET_EVENT)
class OrderBookingResultSetEvent(
    val orderId: UUID,
    val bookingId: UUID,
    val success: Boolean,
    val totalPrice: Int?,
    createdAt: Long = System.currentTimeMillis(),
) : Event<OrderAggregate>(
    name = ORDER_BOOK_RESULTS_SET_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = ORDER_DELIVERY_SET_EVENT)
class OrderDeliverySetEvent(
    val orderId: UUID,
    val deliveryId: UUID,
    createdAt: Long = System.currentTimeMillis(),
) : Event<OrderAggregate>(
    name = ORDER_DELIVERY_SET_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = ORDER_PAYMENT_STARTED_EVENT)
class OrderPaymentStartedEvent(
    val orderId: UUID,
    val paymentId: UUID,
    val amount: Int,
    createdAt: Long = System.currentTimeMillis(),
) : Event<OrderAggregate>(
    name = ORDER_PAYMENT_STARTED_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = ORDER_PAYMENT_FAILED_EVENT)
class OrderPaymentFailedEvent(
    val orderId: UUID,
    val paymentId: UUID,
    val txId: UUID?,
    val reason: String?,
    createdAt: Long = System.currentTimeMillis(),
) : Event<OrderAggregate>(
    name = ORDER_PAYMENT_FAILED_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = ORDER_PAYMENT_SUCCEEDED_EVENT)
class OrderPaymentSucceededEvent(
    val orderId: UUID,
    val paymentId: UUID,
    createdAt: Long = System.currentTimeMillis(),
) : Event<OrderAggregate>(
    name = ORDER_PAYMENT_SUCCEEDED_EVENT,
    createdAt = createdAt,
)
