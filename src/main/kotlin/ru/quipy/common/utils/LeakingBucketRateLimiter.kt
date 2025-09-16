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
import java.util.concurrent.TimeUnit

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    private val bucketSize: Int,
) : RateLimiter {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }

    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Int>(bucketSize)
    private val leakIntervalMs = window.toMillis() / rate

    override fun tick(): Boolean {
        return queue.offer(1)
    }

    override fun tickBlocking() {
        try {
            queue.put(1)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw RuntimeException("Interrupted while waiting for bucket space", e)
        }
    }

    override fun tickBlocking(timeout: Duration): Boolean {
        return try {
            queue.offer(1, timeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            delay(leakIntervalMs)
            queue.poll() // Remove one token from the bucket
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
}