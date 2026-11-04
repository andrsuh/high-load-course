package ru.quipy.apigateway

import org.slf4j.LoggerFactory
import ru.quipy.common.utils.RateLimiter

class IngressRateLimiter(
    private val shortDeadlineDelegate: RateLimiter?,
    private val longDeadlineDelegate: RateLimiter? = null,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(IngressRateLimiter::class.java)

        private const val WAIT_STEP_MS = 5L
    }

    val enabled: Boolean get() = shortDeadlineDelegate != null || longDeadlineDelegate != null

    fun tryAcquire(waitBudgetMs: Long = 0L, longDeadline: Boolean = false): Boolean {
        val limiter = if (longDeadline) longDeadlineDelegate ?: shortDeadlineDelegate else shortDeadlineDelegate
        if (limiter == null) return true

        val waitUntil = System.nanoTime() + waitBudgetMs * 1_000_000L
        while (true) {
            val allowed = try {
                limiter.tick()
            }
            catch (e: Exception) {
                logger.error("Ingress rate limiter failed, request is allowed", e)
                true
            }

            if (allowed) return true
            if (System.nanoTime() >= waitUntil) return false
            Thread.sleep(WAIT_STEP_MS)
        }
    }
}
