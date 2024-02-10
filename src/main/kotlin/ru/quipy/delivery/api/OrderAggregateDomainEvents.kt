package ru.quipy.delivery.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.util.*

const val DELIVERY_CREATED_EVENT = "DELIVERY_CREATED_EVENT"

@DomainEvent(name = DELIVERY_CREATED_EVENT)
class DeliveryCreatedEvent(
    val deliveryId: UUID,
    val orderId: UUID,
    val deliveryDuration: Long,
    createdAt: Long = System.currentTimeMillis(),
) : Event<DeliveryAggregate>(
    name = DELIVERY_CREATED_EVENT,
    createdAt = createdAt,
)