package ru.quipy.common.exceptions

class PaymentException : Exception {

    private constructor(message: String) : super(message) { }

    companion object {
        fun paymentFailure(message: String) {
            throw PaymentException(message)
        }
    }

}