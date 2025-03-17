package ru.quipy.payments.logic.PaymentStages


import ru.quipy.common.utils.RateLimiter
import ru.quipy.payments.logic.PaymentStages.StageMarkers.RateLimitMarker
import ru.quipy.payments.logic.PaymentStages.StageResults.ProcessResult

class RateLimitStage(val next: PaymentStage<*, ProcessResult>, val rateLimiter: RateLimiter) : PaymentStage<RateLimitMarker, ProcessResult> {
    override suspend fun process(payment: Payment): ProcessResult {
        while (!rateLimiter.tick()) { Unit }

         return next.process(payment)
    }
}