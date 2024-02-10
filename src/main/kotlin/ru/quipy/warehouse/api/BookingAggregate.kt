package ru.quipy.warehouse.api

import ru.quipy.core.annotations.AggregateType
import ru.quipy.domain.Aggregate

@AggregateType(aggregateEventsTableName = "aggregate-booking")
class BookingAggregate : Aggregate