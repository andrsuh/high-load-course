package ru.quipy.delivery.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.delivery.api.DeliveryAggregate
import ru.quipy.delivery.api.DeliveryCreatedEvent
import ru.quipy.domain.AggregateState
import java.util.*

class DeliveryAggregateState : AggregateState<UUID, DeliveryAggregate> {
    private lateinit var deliveryId: UUID
    var deliveryDuration: Long? = null
    var createdAt: Long = System.currentTimeMillis()
    var updatedAt: Long = System.currentTimeMillis()

    override fun getId() = deliveryId

    @StateTransitionFunc
    fun deliveryCreatedApply(event: DeliveryCreatedEvent) {
        deliveryId = event.deliveryId
        deliveryDuration = event.deliveryDuration
        updatedAt = createdAt
    }
}