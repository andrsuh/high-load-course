package ru.quipy.warehouse.logic

import ru.quipy.warehouse.api.ProductCreatedEvent
import java.util.*


fun ProductAggregateState.create(id: UUID, title: String, price: Int): ProductCreatedEvent {
    return ProductCreatedEvent(
        productID = id,
        title = title,
        price = price
    )
}