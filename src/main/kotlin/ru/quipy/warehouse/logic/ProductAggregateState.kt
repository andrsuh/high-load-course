package ru.quipy.warehouse.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.warehouse.api.*
import java.util.*

class ProductAggregateState : AggregateState<UUID, ProductAggregate> {
    private lateinit var productId: UUID
    var createdAt: Long = System.currentTimeMillis()
    var updatedAt: Long = System.currentTimeMillis()

    lateinit var productTitle: String

    override fun getId() = productId

    @StateTransitionFunc
    fun productCreatedApply(event: ProductCreatedEvent) {
        productId = event.productID
        productTitle = event.title
        updatedAt = createdAt
    }
}