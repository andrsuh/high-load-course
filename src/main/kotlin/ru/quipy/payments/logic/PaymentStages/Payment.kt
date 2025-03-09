package ru.quipy.payments.logic.PaymentStages

import java.util.UUID

data class Payment(val paymentId: UUID, val amount: Int, val paymentStartedAt: Long, val deadline: Long)
