package ru.quipy.common.utils

import java.util.concurrent.Executors

class DoubleSummary(private val initial: Double, private val activeAfterNExecutions: Int = 150) {
    private val executor = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory("summary-updater"))
    private var sum: Double = 0.0
    private var count: Long = 0

    fun reportExecution(duration: Double) {
        executor.execute {
            sum += duration
            count++
        }
    }

    fun getAverage(): Double {
        if (activeAfterNExecutions < 30)
            return initial

        return sum / count
    }
}