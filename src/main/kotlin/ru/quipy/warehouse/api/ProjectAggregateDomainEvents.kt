package ru.quipy.warehouse.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.util.*

const val PRODUCT_CREATED_EVENT = "PRODUCT_CREATED_EVENT"

@DomainEvent(name = PRODUCT_CREATED_EVENT)
class ProductCreatedEvent(
    val productID: UUID,
    val price: Int,
    val title: String,
    createdAt: Long = System.currentTimeMillis(),
) : Event<ProductAggregate>(
    name = PRODUCT_CREATED_EVENT,
    createdAt = createdAt,
)