package ru.quipy.common.utils.ratelimiter

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class FixedWindowRateLimiter(
    private val rate: Int,
    private val window: Long,
    private val timeUnit: TimeUnit = TimeUnit.MINUTES,
) : RateLimiter {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(FixedWindowRateLimiter::class.java)
        private val counter = AtomicInteger(0)
    }

    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private var semaphore = Semaphore(rate)
    private val semaphoreNumber = counter.getAndIncrement()

    private var start = System.currentTimeMillis()
    private var nextExpectedWakeUp = start + timeUnit.toMillis(window)


    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            start = System.currentTimeMillis()
            nextExpectedWakeUp = start + timeUnit.toMillis(window)

            val permitsToRelease = rate - semaphore.availablePermits()
            repeat(permitsToRelease) {
                runCatching {
                    semaphore.release()
                }.onFailure { th -> logger.error("Failed while releasing permits", th) }
            }
            logger.trace("Semaphore ${semaphoreNumber}. Released $permitsToRelease permits")

            delay(nextExpectedWakeUp - System.currentTimeMillis())
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    override fun tick() = semaphore.tryAcquire()

    fun tickBlocking() = semaphore.acquire()
}
