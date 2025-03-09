package ru.quipy.payments.logic.PaymentStages


import ru.quipy.common.utils.RateLimiter
import ru.quipy.payments.logic.PaymentStages.StageMarkers.RateLimitMarker
import ru.quipy.payments.logic.PaymentStages.StageResults.ProcessResult

class RateLimitStage(val next: PaymentStage<*, Unit>, val rateLimiter: RateLimiter) : PaymentStage<RateLimitMarker, Unit> {
    override suspend fun process(payment: Payment) {
        while (!rateLimiter.tick()) { Unit }

         next.process(payment)
    }
}