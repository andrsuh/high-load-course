package ru.quipy.orders.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.orders.api.*
import ru.quipy.orders.logic.OrderStatus.*
import java.util.*

class OrderAggregateState : AggregateState<UUID, OrderAggregate> {
    private lateinit var orderId: UUID

    lateinit var userId: UUID

    var createdAt: Long = System.currentTimeMillis()
    var updatedAt: Long = System.currentTimeMillis()

    var status: OrderStatus = COLLECTING
    var shoppingCart = ShoppingCart()

    var bookingId: UUID? = null
    var priceToPay: Int? = null

    var deliveryId: UUID? = null
    var paymentId: UUID? = null

    override fun getId() = orderId

    @StateTransitionFunc
    fun orderCreatedApply(event: OrderCreatedEvent) {
        orderId = event.orderId
        userId = event.userId
        priceToPay = event.amount
        updatedAt = createdAt
    }

    @StateTransitionFunc
    fun itemAddedApply(event: ItemAddedEvent) {
        shoppingCart.add(event.item)
        updatedAt = createdAt
    }

    @StateTransitionFunc
    fun checkoutCancelledApply(event: CheckoutCancelledEvent) {
        status = COLLECTING
        bookingId = null
        priceToPay = null
        deliveryId = null
        updatedAt = createdAt
    }

    @StateTransitionFunc
    fun checkoutApply(event: OrderCheckoutStartedEvent) {
        status = BOOKING_IN_PROGRESS
        bookingId = event.bookingId
        updatedAt = createdAt
    }

    @StateTransitionFunc
    fun bookingResultsApply(event: OrderBookingResultSetEvent) {
        if (event.success) {
            bookingId = event.bookingId
            priceToPay = event.totalPrice
            status = BOOKED
        } else {
            bookingId = null
            deliveryId = null
            status = COLLECTING
        }
    }

    @StateTransitionFunc
    fun setDeliveryApply(event: OrderDeliverySetEvent) {
        deliveryId = event.deliveryId
        status = DELIVERY_SET
    }

    @StateTransitionFunc
    fun startPaymentApply(event: OrderPaymentStartedEvent) {
        status = PAYMENT_IN_PROGRESS
        paymentId = event.paymentId
    }

    @StateTransitionFunc
    fun paymentFailedApply(event: OrderPaymentFailedEvent) {
        status = PAYMENT_FAILED
        paymentId = event.paymentId
    }

    @StateTransitionFunc
    fun paymentSucceededApply(event: OrderPaymentSucceededEvent) {
        status = PAYED
        paymentId = event.paymentId
    }
}

class CartItem(
    val productId: UUID,
    val amount: Int,
)

class ShoppingCart(val items: MutableList<CartItem> = mutableListOf<CartItem>()) {

    fun add(item: CartItem) {
        items.removeIf { it.productId == item.productId }
        items.add(item)
    }
}

enum class OrderStatus {
    COLLECTING,
    BOOKING_IN_PROGRESS,
    BOOKED,
    DELIVERY_SET,
    PAYMENT_FAILED,
    PAYMENT_IN_PROGRESS,
    PAYED,
}