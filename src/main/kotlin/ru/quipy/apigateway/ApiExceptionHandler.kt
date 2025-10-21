package ru.quipy.apigateway
import io.github.resilience4j.ratelimiter.RequestNotPermitted
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseStatus
import ru.quipy.common.utils.RateLimitExceededException


@ControllerAdvice
class ApiExceptionHandler {

    private val logger = LoggerFactory.getLogger(ApiExceptionHandler::class.java)

    @ExceptionHandler(RequestNotPermitted::class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    fun handleRequestNotPermitted(ex: RequestNotPermitted) {
        logger.warn("Rate limit exceeded!", ex.message ?: "unknown")
    }

    @ExceptionHandler(RateLimitExceededException::class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    fun handleRateLimitExceeded(ex: RateLimitExceededException) {
        logger.warn("Custom rate limit exceeded: {}", ex.message)
    }
}