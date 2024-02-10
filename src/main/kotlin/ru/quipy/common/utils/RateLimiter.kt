package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.payments.logic.now
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class CoroutineRateLimiter(
    private val rate: Int,
    private val timeUnit: TimeUnit = TimeUnit.MINUTES
) {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CoroutineRateLimiter::class.java)
        private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    }

    private val semaphore = Semaphore(rate)

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val permitsToRelease = rate - semaphore.availablePermits
            repeat(permitsToRelease) {
                runCatching {
                    semaphore.release()
                }.onFailure { th -> logger.error("Failed while releasing permits", th) }
            }
            logger.debug("Released $permitsToRelease permits")
            delay(timeUnit.toMillis(1))
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    fun tick() = semaphore.tryAcquire()

    suspend fun tickBlocking() = semaphore.acquire()
}

class RateLimiter(
    private val rate: Int,
    private val timeUnit: TimeUnit = TimeUnit.SECONDS
) {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CoroutineRateLimiter::class.java)
        private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    }

    private val semaphore = java.util.concurrent.Semaphore(rate, true)

    private var lastUpdatedTs = now()

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val started = now()
            val prevUpdate = lastUpdatedTs

            val permitsToRelease = rate - semaphore.availablePermits()
            runCatching {
                semaphore.release(permitsToRelease)
            }.onFailure { th -> logger.error("Failed while releasing permits", th) }
            lastUpdatedTs = now()

            logger.debug("Released $permitsToRelease permits. Between updates: ${lastUpdatedTs - prevUpdate} ms")
            delay(timeUnit.toMillis(1) - (now() - started))
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    fun tick() = semaphore.tryAcquire()

    fun tickBlocking() = semaphore.acquire()
}

class CountingRateLimiter(
    private val rate: Int,
    private val timeUnit: TimeUnit = TimeUnit.SECONDS
) {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CoroutineRateLimiter::class.java)
    }

    private val timePeriodMillis = timeUnit.toMillis(1)

    var internal = RlInternal()

    @Synchronized
    fun tick(): Boolean {
        val now = now()
        if (now - internal.segmentStart > timePeriodMillis) {
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
        var segmentStart: Long = now(),
        var permits: Int = 0,
    )
}