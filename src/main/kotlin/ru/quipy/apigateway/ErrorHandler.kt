package ru.quipy.apigateway

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import java.util.concurrent.RejectedExecutionException

@ControllerAdvice
class ErrorHandler {

    @ExceptionHandler
    fun handleRejectedExecutionException(e: RejectedExecutionException): ResponseEntity<String> {
        val headers = HttpHeaders()
        val retryAfterSeconds = 15
        headers.add("Retry-After", retryAfterSeconds.toString())

        return ResponseEntity("Too many requests", headers, HttpStatus.TOO_MANY_REQUESTS)
    }
}