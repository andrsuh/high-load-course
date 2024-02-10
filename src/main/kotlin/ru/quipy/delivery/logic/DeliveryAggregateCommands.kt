package ru.quipy.delivery.logic

import ru.quipy.delivery.api.DeliveryCreatedEvent
import java.util.*


fun DeliveryAggregateState.create(id: UUID, orderId: UUID, deliveryDuration: Long): DeliveryCreatedEvent {
    return DeliveryCreatedEvent(
        deliveryId = id,
        orderId = orderId,
        deliveryDuration = deliveryDuration
    )
}