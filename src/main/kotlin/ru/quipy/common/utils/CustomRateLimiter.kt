package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class CustomRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private var bucket: AtomicInteger = AtomicInteger(0)

    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    override fun tick(): Boolean {
        return true
    }

    suspend fun tickBlocking(deadline: Long): Boolean {
        var size = bucket.get()
        if (System.currentTimeMillis() + size * window.toMillis() >= deadline) {
            return false
        }
        size = bucket.getAndAdd(1)
        delay(size * window.toMillis() / rate)
        return true
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            delay(window.toMillis() / rate)
            bucket.getAndUpdate {
                if (it > 0) it - 1 else it
            }
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }

    class WaitCondition(var value: Boolean)
}