package ru.quipy.orders.api

import ru.quipy.core.annotations.AggregateType
import ru.quipy.domain.Aggregate

@AggregateType(aggregateEventsTableName = "aggregate-order")
class OrderAggregate : Aggregate