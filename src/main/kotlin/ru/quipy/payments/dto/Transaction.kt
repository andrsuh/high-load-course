package ru.quipy.payments.dto

import java.util.UUID

data class Transaction(val orderId: UUID, val amount: Int, val paymentId: UUID, val deadline: Long)
