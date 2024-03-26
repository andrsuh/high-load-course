package ru.quipy.warehouse.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.warehouse.api.*
import java.util.*

class BookingAggregateState : AggregateState<UUID, BookingAggregate> {
    lateinit var bookingId: UUID
    var createdAt: Long = System.currentTimeMillis()
    var updatedAt: Long = System.currentTimeMillis()
    var state = BookingState.CREATED

    lateinit var orderId: UUID
    var products: List<BookProductRequest> = listOf()
    var succeededProducts: List<BookedProduct> = listOf()
    var failedProducts: List<FailedProduct> = listOf()

    override fun getId() = bookingId

    @StateTransitionFunc
    fun bookingCreatedApply(event: BookingCreatedEvent) {
        bookingId = event.bookingId
        orderId = event.orderId
        products = event.products
        updatedAt = createdAt
    }

    @StateTransitionFunc
    fun bookingSucceededApply(event: BookingSucceededEvent) {
        updatedAt = createdAt
        succeededProducts = event.succeededProducts
        state = BookingState.SUCCESS
    }

    @StateTransitionFunc
    fun bookingFailedApply(event: BookingFailedEvent) {
        failedProducts = event.failedProducts
        state = BookingState.FAILED
        updatedAt = createdAt
    }

    @StateTransitionFunc
    fun bookingCancelledApply(event: BookingUnbookedEvent) {
        state = BookingState.CANCELLED
        updatedAt = createdAt
    }
}

data class BookProductRequest(
    val productId: UUID,
    val amount: Int,
)
data class BookedProduct(
    val productId: UUID,
    val amount: Int,
    val price: Int,
)

data class FailedProduct(
    val productId: UUID,
    val amount: Int,
    val reason: String
)

enum class BookingState {
    CREATED,
    SUCCESS,
    FAILED,
    CANCELLED,
}
