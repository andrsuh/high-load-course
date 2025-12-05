package ru.quipy.payments.api

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import ru.quipy.payments.exceptions.TooManyRequestsException

@ControllerAdvice
class GlobalExceptionHandler {

    val logger: Logger = LoggerFactory.getLogger(GlobalExceptionHandler::class.java)

    @ExceptionHandler(TooManyRequestsException::class)
    fun handleTooManyRequests(ex: TooManyRequestsException): ResponseEntity<String> {

        logger.error("Exception message: ${ex.message} . We will retry after ${ex.retryAfterSeconds}")
        logger.error(ex.message)

        return ResponseEntity
            .status(HttpStatus.TOO_MANY_REQUESTS)
            .header("Retry-After", ex.retryAfterSeconds.toString())
            .body(ex.message)
    }
}
