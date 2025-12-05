package ru.quipy.common.utils

import org.slf4j.Logger
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import ru.quipy.payments.exceptions.TooManyRequestsException

class DeadlineAndRateLimitGuard(
    private val logger: Logger,
    private val limiter: SlidingWindowRateLimiter,
    private val averageProcessingTimeMs: Long,
) {

    fun check(deadline: Long, now: Long, retryAfter: Long, onTooManyRequests: () -> Unit = {}) {
        logger.info("Условие: [deadline: $deadline, averageProcessingTime: $averageProcessingTimeMs, now: $now]")

        if (deadline <= now + averageProcessingTimeMs) {
            logger.info(
                "Условие не в лимитере: [deadline <= now + averageProcessingTime] — выбрасываем GONE"
            )

            throw ResponseStatusException(
                HttpStatus.GONE,
                "Deadline expired before request could be processed"
            )
        }

        if (!limiter.tick()) {
            logger.info("Лимитер отклонил запрос. Повторная проверка дедлайна.")

            if (deadline > now + averageProcessingTimeMs) {
                logger.info("Условие '[deadline > now + averageProcessingTime]' — выбрасываем TOO_MANY_REQUESTS")

                onTooManyRequests()

                throw TooManyRequestsException(
                    retryAfter,
                    "Rate limit exceeded. Try again later."
                )
            }

            logger.info(
                "Условие '[deadline <= now + averageProcessingTime]' — выбрасываем GONE"
            )
            throw ResponseStatusException(
                HttpStatus.GONE,
                "Deadline expired before request could be processed"
            )
        }
    }
}
