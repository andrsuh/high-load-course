package ru.quipy.common.utils

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

class CustomTokenBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val availableTokens = AtomicLong(bucketSize.toLong())

    init {
        startTokenRefillJob()
    }

    override fun tick(): Boolean {
        val current = availableTokens.get()
        return current > 0 && availableTokens.compareAndSet(current, current - 1)
    }

    suspend fun tickBlocking(timeout: Duration): Boolean {
        return suspendCoroutine { cont ->
            val start = System.currentTimeMillis()
            while (!tick()) {
                if (System.currentTimeMillis() - start > timeout.toMillis()) {
                    cont.resume(false)
                    return@suspendCoroutine
                }
            }
            cont.resume(true)
        }
    }

    private fun startTokenRefillJob() {
        rateLimiterScope.launch {
            while (isActive) {
                delay(window.toMillis() / rate)
                val current = availableTokens.get()
                if (current < rate) {
                    availableTokens.incrementAndGet()
                }
            }
        }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter token refill job failed", th) }
    }


    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CustomTokenBucketRateLimiter::class.java)
    }
}