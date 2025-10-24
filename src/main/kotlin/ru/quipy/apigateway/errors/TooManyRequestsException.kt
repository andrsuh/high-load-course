package ru.quipy.apigateway.errors

import java.util.UUID

data class TooManyRequestsException(
    val uuid: UUID,
    val attempts: Int
): RuntimeException()
