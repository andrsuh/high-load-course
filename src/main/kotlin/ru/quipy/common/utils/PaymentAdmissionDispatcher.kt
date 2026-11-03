package ru.quipy.common.utils

import java.util.concurrent.Executor
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

class PaymentAdmissionDispatcher(
    private val limiter: PaymentAdmissionLimiter,
    private val executor: Executor,
    private val onRejected: (RejectedExecutionException) -> Unit = {},
) : AutoCloseable {
    private val scheduler = ScheduledThreadPoolExecutor(1, NamedThreadFactory("payment-admission-scheduler"))
    val pendingCount: Int get() = scheduler.queue.size

    fun submit(maxWaitMillis: Long, processingAllowanceMillis: Long, task: Runnable): Boolean {
        val at = limiter.reserve(maxWaitMillis, processingAllowanceMillis) ?: return false
        try {
            scheduler.schedule({
                try {
                    val runAndRelease = {
                        try { task.run() }
                        finally { limiter.complete() }
                    }
                    val admittedTask = if (task is PaymentTask) {
                        PaymentTask(task.amount, task.latestStart, task.sequence, runAndRelease)
                    } else Runnable { runAndRelease() }
                    executor.execute(admittedTask)
                } catch (e: RejectedExecutionException) {
                    limiter.complete()
                    onRejected(e)
                }
            }, (at - System.nanoTime()).coerceAtLeast(0), TimeUnit.NANOSECONDS)
        } catch (e: RejectedExecutionException) {
            limiter.complete()
            return false
        }
        return true
    }

    override fun close() {
        scheduler.shutdownNow().forEach { limiter.complete() }
    }
}
