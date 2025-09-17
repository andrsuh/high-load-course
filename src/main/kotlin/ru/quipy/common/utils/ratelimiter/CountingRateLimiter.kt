package ru.quipy.common.utils.ratelimiter

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class CountingRateLimiter(
    private val rate: Int,
    private val window: Long,
    private val timeUnit: TimeUnit = TimeUnit.SECONDS
) : RateLimiter {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CountingRateLimiter::class.java)
    }

    var internal = RlInternal(System.currentTimeMillis(), rate)

    @Synchronized
    override fun tick(): Boolean {
        val now = System.currentTimeMillis()
        if (now - internal.segmentStart >= timeUnit.toMillis(window)) {
            internal = RlInternal(now, rate - 1)
            return true
        } else {
            if (internal.permits > 0) {
                internal.permits--
                return true
            } else {
                return false
            }
        }
    }

    class RlInternal(
        var segmentStart: Long = System.currentTimeMillis(),
        var permits: Int = 0,
    )
}
