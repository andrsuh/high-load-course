package ru.quipy.common.utils

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.server.ResponseStatusException

data class ErrorMessageModel(
    var status: Int? = null,
    var message: String? = null
)

@ControllerAdvice
class ExceptionHandler {

    @ExceptionHandler
    fun handleResponseStatusException(ex: ResponseStatusException): ResponseEntity<ErrorMessageModel> {
        val errorMessage = ErrorMessageModel(
            ex.statusCode.value(),
            ex.message
        )

        val headers = HttpHeaders().apply {
            if (ex.statusCode == HttpStatus.TOO_MANY_REQUESTS && ex.reason != null) {
                add("Retry-After", ex.reason)
            }
        }

        return ResponseEntity(errorMessage, headers, ex.statusCode)
    }
}
