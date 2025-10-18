package ru.quipy.payments.logic

data class PaymentRequest(
    val deadline: Long,
    val call: Runnable,
) : Comparable<PaymentRequest> {
    override fun compareTo(other: PaymentRequest): Int = deadline.compareTo(other.deadline)
}