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
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Int>(bucketSize)
    private var futureBurst: Long =  System.currentTimeMillis()

    override fun tick(): Boolean {
        return queue.offer(1)
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            futureBurst += window.toMillis()
            delay(window.toMillis())
            for (i in 1..rate) {
                queue.poll()
            }
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }

    fun size() : Int {
        return queue.size
    }

    fun burst() : Long {
        return futureBurst
    }

    val rateLimiterQueueCounter: Gauge = Gauge.builder(
        "requests_in_queue_total",
        java.util.function.Supplier {  queue.size.toDouble() }
    )
        .description("Total number of payment requests in queue")
        .tag("queue", "incoming rate limiter leakyBucket")
        .register(Metrics.globalRegistry)
}