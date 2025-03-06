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
    private val rps: Long,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Unit>(rps.toInt())

    override fun tick(): Boolean {
        return queue.offer(Unit)
    }

    fun tickBlocking(duration: Long): Boolean {
        return queue.offer(Unit, duration, TimeUnit.MILLISECONDS)
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            delay((Duration.ofSeconds(1).toMillis()) / rps)
            queue.poll()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}