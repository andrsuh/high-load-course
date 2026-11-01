package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import kotlin.time.Duration.Companion.milliseconds

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    private val bucketSize: Int,
) : RateLimiter {
    init {
        require(rate > 0) { "rate must be positive" }
        require(!window.isZero && !window.isNegative) { "window must be positive" }
        require(bucketSize > 0) { "bucketSize must be positive" }
    }

    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Int>(bucketSize)

    override fun tick(): Boolean {
        return queue.offer(1)
    }

    init {
        rateLimiterScope.launch {
            val intervalMs = (((window.toMillis() + rate - 1) / rate) + 1L).coerceAtLeast(1L)
            logger.warn(
                "Leaking bucket started: rate={} per {}, leak interval={} ms, bucket={}",
                rate, window, intervalMs, bucketSize
            )
            while (true) {
                delay(intervalMs.milliseconds)
                queue.poll()
            }
        }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}
