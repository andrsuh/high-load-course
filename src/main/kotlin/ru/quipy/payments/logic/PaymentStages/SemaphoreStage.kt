package ru.quipy.payments.logic.PaymentStages

import ru.quipy.payments.logic.PaymentStages.StageMarkers.SemaphoreMarker
import ru.quipy.payments.logic.PaymentStages.StageResults.ProcessResult

class SemaphoreStage(val next: PaymentStage<*, ProcessResult>) : PaymentStage<SemaphoreMarker, ProcessResult> {
    override suspend fun process(payment: Payment) : ProcessResult {
        return next.process(payment)
    }
}