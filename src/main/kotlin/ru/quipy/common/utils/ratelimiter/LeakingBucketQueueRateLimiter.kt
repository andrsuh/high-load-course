package ru.quipy.common.utils.ratelimiter

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

class LeakingBucketQueueRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int
) {

    private val scope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val queue = LinkedBlockingQueue<suspend () -> Unit>(bucketSize)

    fun enqueue(request: suspend () -> Unit): Boolean {
        val offered = queue.offer(request)
        if (!offered) {
            logger.warn("LeakyBucket: queue full, rejecting request")
        }
        return offered
    }

    private val releaseJob = scope.launch {
            val delayPerRequest = window.inWholeMilliseconds / rate
            logger.info("LeakyBucket: starting leak job with rate=$rate per $window (delay=$delayPerRequest ms)")

            while (isActive) {
                val task = queue.poll()
                if (task != null) {
                    try {
                        task()
                    } catch (ex: Exception) {
                        logger.error("LeakyBucket: task execution failed", ex)
                    }
                }
                delay(delayPerRequest)
            }
        }.invokeOnCompletion { th ->
            if (th != null) logger.error("LeakyBucket: leak job stopped unexpectedly", th)
        }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketQueueRateLimiter::class.java)
    }
}
