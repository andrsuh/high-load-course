package ru.quipy.common.utils

import java.util.concurrent.atomic.AtomicLong

class AverageTimeKeeper {
    private val average = AtomicLong(0)

    private val theta = 0.1;

    fun record(processingTimeMillis: Long) {
        while (true) {
            val current = average.get()
            var newCur: Double
            if (current == 0L)
                newCur = processingTimeMillis.toDouble()
            else {
                newCur = processingTimeMillis.toDouble() * theta + current.toDouble() * (1.0 - theta)
            }

            if (average.compareAndSet(current, newCur.toLong())) {
                break
            }
        }
    }

    fun getAverage(): Long {
        return average.get()
    }
}