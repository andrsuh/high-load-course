package ru.quipy.common.utils

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class GlobalExceptionHandler(
    private val maxWait: String = "3",
) {
    companion object {
        val logger = LoggerFactory.getLogger(GlobalExceptionHandler::class.java)
    }

    @ExceptionHandler(TooManyRequestsException::class)
    fun handleTooManyRequests(ex: TooManyRequestsException): ResponseEntity<String> {
        return ResponseEntity
            .status(HttpStatus.TOO_MANY_REQUESTS)
            .header("Retry-After", maxWait)
            .body(ex.message ?: "Повторите позже")
    }
}