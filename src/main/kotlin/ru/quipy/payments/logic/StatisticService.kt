package ru.quipy.payments.logic

import java.util.*

class StatisticService {
    private val timesArr = LinkedList<Long>();
    private var percentile95: Long = 0
    private val maxSize = 100


    @Synchronized
    public fun addTime(time: Long) {
        timesArr.add(time);
        if (timesArr.size >= maxSize) {
            timesArr.removeFirst();
        }
        updateStats();
    }

    private fun calculatePercentile(data: List<Long>, percentile: Double): Long {
        if (data.isEmpty()) throw Exception("Data size must be more than 0")
        val sortedData = data.sorted()
        val index = (percentile / 100.0 * (sortedData.size - 1)).toInt()
        return sortedData[index]
    }

    @Synchronized
    private fun updateStats() {
        percentile95 = calculatePercentile(timesArr, 0.05)
    }

    fun getPercentile95(): Long {
        return percentile95;
    }

}