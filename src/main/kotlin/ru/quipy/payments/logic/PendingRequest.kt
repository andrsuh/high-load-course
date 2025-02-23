package ru.quipy.payments.logic

import java.util.*

class PendingRequest(
    val paymentId: UUID, val amount: Int, val paymentStartedAt: Long, val deadline: Long
) : Comparable<PendingRequest> {
    override fun compareTo(other: PendingRequest): Int {
        return deadline.compareTo(other.deadline)
    }
}