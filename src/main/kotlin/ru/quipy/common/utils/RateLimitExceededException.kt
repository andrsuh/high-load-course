package ru.quipy.common.utils

class RateLimitExceededException(
    message: String = "Rate limit exceeded",
    val retryMs: Long = 5000
) : RuntimeException(message) {
}