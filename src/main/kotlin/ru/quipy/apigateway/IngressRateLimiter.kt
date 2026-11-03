package ru.quipy.apigateway

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.RateLimiter

class IngressRateLimiter(
    private val shortDeadlineDelegate: RateLimiter?,
    private val longDeadlineDelegate: RateLimiter? = null,
    meterRegistry: MeterRegistry,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(IngressRateLimiter::class.java)

        private const val WAIT_STEP_MS = 5L
    }

    private val admitted = meterRegistry.counter("ingress_rate_limiter_total", "result", "admitted")
    private val rejected = meterRegistry.counter("ingress_rate_limiter_total", "result", "rejected")
    private val failed = meterRegistry.counter("ingress_rate_limiter_total", "result", "failed_open")

    val enabled: Boolean get() = shortDeadlineDelegate != null || longDeadlineDelegate != null

    fun tryAcquire(waitBudgetMs: Long = 0L, longDeadline: Boolean = false): Boolean {
        val limiter = if (longDeadline) longDeadlineDelegate ?: shortDeadlineDelegate else shortDeadlineDelegate
        if (limiter == null) {
            admitted.increment()
            return true
        }

        val waitUntil = System.nanoTime() + waitBudgetMs * 1_000_000L
        while (true) {
            val allowed = try {
                limiter.tick()
            }
            catch (e: Exception) {
                failed.increment()
                logger.error("Ingress rate limiter failed, request is allowed", e)
                true
            }

            if (allowed) {
                admitted.increment()
                return true
            }
            if (System.nanoTime() >= waitUntil) {
                rejected.increment()
                return false
            }
            Thread.sleep(WAIT_STEP_MS)
        }
    }
}
