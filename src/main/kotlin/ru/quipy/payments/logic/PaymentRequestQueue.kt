package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingQuantile
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.*
import kotlin.math.max

enum class PaymentRejectPhase {
    ENQUEUE,
    DISPATCH,
}

class PaymentRequestQueue(
    name: String,
    rateLimitPerSec: Int,
    parallelRequests: Int,
    queueCapacity: Int,
    private val meterRegistry: MeterRegistry,
    private val averageProcessingTime: Duration,
    private val onExecute: (PaymentTask) -> Unit,
    private val onReject: (PaymentTask, String, PaymentRejectPhase) -> Unit,
) {
    companion object {
        private val logger =
            LoggerFactory.getLogger(PaymentRequestQueue::class.java)
    }

    init {
        require(rateLimitPerSec > 0)
        require(parallelRequests > 0)
        require(queueCapacity > 0)
        require(averageProcessingTime.toMillis() > 0)
    }

    private val limiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1),
    )

    private val avgMillis = averageProcessingTime.toMillis()

    private val workerLimit = minOf(parallelRequests, 256)

    private val throughputPerSec = minOf(
        rateLimitPerSec.toDouble(),
        workerLimit * 1000.0 / avgMillis,
    )

    private val queue = ArrayBlockingQueue<PaymentTask>(queueCapacity)

    private val queueSizeMetric = Gauge.builder("payment_request_queue_size") { queue.size.toDouble() }
        .tags("account", name)
        .register(meterRegistry);

    private val processingTimes = SlidingQuantile(
        windowSize = 1000,
        bucketWidth = max(1L, avgMillis / 10),
        numBuckets = 100,
    )

    private fun reserveMillis(): Long {
        if (processingTimes.count() < 30) {
            return (avgMillis * 1.5).toLong()
        }

        return processingTimes.quantile(0.97)
    }

    fun pendingRequests(): Int = queue.size

    private fun reject(
        task: PaymentTask,
        reason: String,
        phase: PaymentRejectPhase,
    ) {
        try {
            onReject(task, reason, phase)
        } catch (e: Exception) {
            logger.error(
                "Failed to record rejection for payment ${task.paymentId}, " +
                        "phase=$phase, reason=$reason",
                e,
            )
        }
    }

    fun submit(task: PaymentTask) {
        val now = System.currentTimeMillis()
        val remaining = task.deadline - now - reserveMillis()

        if (remaining <= 0) {
            reject(
                task,
                "not enough time left at enqueue (now: $now)",
                PaymentRejectPhase.ENQUEUE,
            )
            return
        }

        val maxQueueAhead =
            remaining / 1000.0 * throughputPerSec

        if (queue.size >= maxQueueAhead) {
            reject(
                task,
                "queue ahead ${queue.size} >= N=$maxQueueAhead " +
                        "(v=$throughputPerSec)",
                PaymentRejectPhase.ENQUEUE,
            )
            return
        }

        if (!queue.offer(task)) {
            reject(
                task,
                "Payment queue overflow",
                PaymentRejectPhase.ENQUEUE,
            )
        }
    }

    @Volatile
    private var running = true

    private val slots = Semaphore(workerLimit)

    private val dispatcher = Thread(
        ::dispatcherLoop,
        "payment-$name-dispatcher",
    ).apply {
        isDaemon = true
        start()
    }

    private val executor = ThreadPoolExecutor(
        0,
        workerLimit,
        30L,
        TimeUnit.SECONDS,
        SynchronousQueue(),
        { runnable ->
            Thread(runnable, "payment-$name-worker").apply {
                isDaemon = true
            }
        },
        ThreadPoolExecutor.AbortPolicy(),
    )

    private fun dispatcherLoop() {
        while (running) {
            val task = try {
                queue.take()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }
            dispatch(task)
        }
    }

    private fun dispatch(task: PaymentTask) {
        try {
            slots.acquire()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()

            reject(
                task,
                "dispatcher interrupted while acquiring worker slot",
                PaymentRejectPhase.DISPATCH,
            )
            return
        }

        var transferredToWorker = false

        try {
            val maxWaitMillis = task.deadline - System.currentTimeMillis() - reserveMillis()

            if (!limiter.tickBlocking(Duration.ofMillis(maxWaitMillis))) {
                reject(
                    task,
                    "no rate limit slot before deadline",
                    PaymentRejectPhase.DISPATCH,
                )
                return
            }

            executor.execute {
                try {
                    val start = System.nanoTime()

                    try {
                        onExecute(task)
                    } catch (e: Exception) {
                        logger.error(
                            "Unexpected worker failure for payment " +
                                    "${task.paymentId}",
                            e,
                        )
                    } finally {
                        val elapsedMillis =
                            TimeUnit.NANOSECONDS.toMillis(
                                System.nanoTime() - start
                            )

                        processingTimes.add(elapsedMillis)
                    }
                } finally {
                    slots.release()
                }
            }

            transferredToWorker = true

        } catch (e: RejectedExecutionException) {
            reject(
                task,
                "worker executor rejected task: ${e.message}",
                PaymentRejectPhase.DISPATCH,
            )
        } finally {
            if (!transferredToWorker) {
                slots.release()
            }
        }
    }
}