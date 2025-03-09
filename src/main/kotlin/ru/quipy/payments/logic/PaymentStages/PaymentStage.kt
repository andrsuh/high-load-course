package ru.quipy.payments.logic.PaymentStages

interface PaymentStage<TMarker, TStageResult> {
    suspend fun process(payment: Payment) : TStageResult
}