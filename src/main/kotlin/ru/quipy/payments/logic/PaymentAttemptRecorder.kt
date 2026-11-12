package ru.quipy.payments.logic

import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID

interface PaymentAttemptRecorder {
    fun submitted(paymentId: UUID, transactionId: UUID, startedAt: Long, spentInQueue: Duration)
    fun processed(paymentId: UUID, transactionId: UUID, success: Boolean, processedAt: Long, reason: String?)
}

class EventSourcedPaymentAttemptRecorder(
    private val payments: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
) : PaymentAttemptRecorder {
    override fun submitted(paymentId: UUID, transactionId: UUID, startedAt: Long, spentInQueue: Duration) {
        payments.update(paymentId) { it.logSubmission(true, transactionId, startedAt, spentInQueue) }
    }

    override fun processed(paymentId: UUID, transactionId: UUID, success: Boolean, processedAt: Long, reason: String?) {
        payments.update(paymentId) { it.logProcessing(success, processedAt, transactionId, reason) }
    }
}
