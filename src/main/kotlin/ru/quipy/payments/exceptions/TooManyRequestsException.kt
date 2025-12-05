package ru.quipy.payments.exceptions

class TooManyRequestsException(
    val retryAfterSeconds: Long,
    message: String
) : RuntimeException(message)
