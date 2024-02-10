package ru.quipy.warehouse.logic

import ru.quipy.warehouse.api.*
import java.util.*


fun BookingAggregateState.create(id: UUID, orderId: UUID, products: List<BookProductRequest>): BookingCreatedEvent {
    return BookingCreatedEvent(
        bookingId = id,
        orderId = orderId,
        products = products
    )
}

fun BookingAggregateState.succeeded(booked: List<BookedProduct>): BookingSucceededEvent {
    return BookingSucceededEvent(
        bookingId = this.bookingId,
        orderId = this.orderId,
        succeededProducts = booked
    )
}

fun BookingAggregateState.failed(failedProduct: List<FailedProduct>): BookingFailedEvent {
    return BookingFailedEvent(
        bookingId = this.bookingId,
        orderId = this.orderId,
        failedProducts = failedProduct
    )
}

fun BookingAggregateState.unbook(): BookingUnbookedEvent {
    return BookingUnbookedEvent(
        bookingId = this.bookingId,
        orderId = this.orderId,
    )
}