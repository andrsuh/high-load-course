package ru.quipy.warehouse.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import ru.quipy.warehouse.logic.BookProductRequest
import ru.quipy.warehouse.logic.BookedProduct
import ru.quipy.warehouse.logic.FailedProduct
import java.util.*

const val BOOKING_CREATED_EVENT = "BOOKING_CREATED_EVENT"
const val BOOKING_SUCCEEDED_EVENT = "BOOKING_SUCCEEDED_EVENT"
const val BOOKING_FAILED_EVENT = "BOOKING_FAILED_EVENT"
const val BOOKING_UNBOOKED_EVENT = "BOOKING_UNBOOKED_EVENT"

@DomainEvent(name = BOOKING_CREATED_EVENT)
class BookingCreatedEvent(
    val bookingId: UUID,
    val orderId: UUID,
    val products: List<BookProductRequest>,
    createdAt: Long = System.currentTimeMillis(),
) : Event<BookingAggregate>(
    name = BOOKING_CREATED_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = BOOKING_SUCCEEDED_EVENT)
class BookingSucceededEvent(
    val bookingId: UUID,
    val orderId: UUID,
    val succeededProducts: List<BookedProduct>,
    createdAt: Long = System.currentTimeMillis(),
) : Event<BookingAggregate>(
    name = BOOKING_SUCCEEDED_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = BOOKING_FAILED_EVENT)
class BookingFailedEvent(
    val bookingId: UUID,
    val orderId: UUID,
    val failedProducts: List<FailedProduct>,
    createdAt: Long = System.currentTimeMillis(),
) : Event<BookingAggregate>(
    name = BOOKING_FAILED_EVENT,
    createdAt = createdAt,
)

@DomainEvent(name = BOOKING_UNBOOKED_EVENT)
class BookingUnbookedEvent(
    val bookingId: UUID,
    val orderId: UUID,
    createdAt: Long = System.currentTimeMillis(),
) : Event<BookingAggregate>(
    name = BOOKING_UNBOOKED_EVENT,
    createdAt = createdAt,
)
