package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

class Summary(private val initial: Double, private val activeAfterNExecutions: Int = 30) {
    private val sum = AtomicLong(0)
    private val count = AtomicLong(0)
    fun reportExecution(duration: Duration) {
        sum.addAndGet(duration.toMillis())
        count.incrementAndGet()
    }

    fun reportExecution(duration: Long) {
        sum.addAndGet(duration)
        count.incrementAndGet()
    }

    fun getAverage(): Double? {
        val c = count.get()
        if (c < activeAfterNExecutions) return null

        return sum.get().toDouble() / count.get()
    }

    fun getAverageMillis(): Duration? {
        val c = count.get()
        if (c < activeAfterNExecutions) return null

        return Duration.ofMillis(sum.get() / count.get())
    }
}