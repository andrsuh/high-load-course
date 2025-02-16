package ru.quipy.orders.subscribers.payment.handlers

import ru.quipy.domain.Event

interface EventHandler<TEvent: Event<*>> {
    fun handle(event: TEvent)
}