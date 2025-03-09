package ru.quipy.common.utils

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler

data class ErrorMessageModel(
    var status: Int? = null,
    var message: String? = null
)

@ControllerAdvice
class ExceptionHandler {
    @ExceptionHandler
    fun handleIllegalStateException(ex: IllegalAccessException): ResponseEntity<ErrorMessageModel> {
        val errorMessage = ErrorMessageModel(
            HttpStatus.TOO_MANY_REQUESTS.value(),
            ex.message
        )
        return ResponseEntity(errorMessage, HttpStatus.TOO_MANY_REQUESTS)
    }

    @ExceptionHandler
    fun handleIllegalStateException(ex: IllegalStateException): ResponseEntity<ErrorMessageModel> {
        val errorMessage = ErrorMessageModel(
            HttpStatus.SERVICE_UNAVAILABLE.value(),
            ex.message
        )
        return ResponseEntity(errorMessage, HttpStatus.SERVICE_UNAVAILABLE)
    }
}