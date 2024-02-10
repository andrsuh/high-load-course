package ru.quipy.payments.api

import ru.quipy.core.annotations.AggregateType
import ru.quipy.domain.Aggregate

@AggregateType(aggregateEventsTableName = "aggregate-payment")
class PaymentAggregate : Aggregate