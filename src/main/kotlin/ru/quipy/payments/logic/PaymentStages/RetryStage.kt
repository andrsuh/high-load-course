package ru.quipy.payments.logic.PaymentStages

import ru.quipy.payments.logic.PaymentStages.StageMarkers.RetryMarker
import ru.quipy.payments.logic.PaymentStages.StageResults.ProcessResult


class RetryStage(
    val next: PaymentStage<*, ProcessResult>
) : PaymentStage<RetryMarker, ProcessResult> {
    private val retryTimes: Int = 3;

    override suspend fun process(payment: Payment): ProcessResult {
        for (x in 0 until retryTimes) {
            val result = next.process(payment)

            if (result.retry)
                continue

            return result
        }
        return ProcessResult(retry = false)
    }

}