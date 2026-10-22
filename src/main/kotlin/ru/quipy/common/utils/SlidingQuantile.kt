package ru.quipy.common.utils

import java.util.ArrayDeque
import kotlin.math.ceil

class SlidingQuantile(
    private val windowSize: Int,
    private val bucketWidth: Long,
    private val numBuckets: Int,
) {

    init {
        require(windowSize > 0)
        require(bucketWidth > 0)
        require(numBuckets > 0)
    }

    private val ring = ArrayDeque<Int>()
    private val counts = IntArray(numBuckets)

    @Synchronized
    fun add(durationMillis: Long) {
        if (ring.size == windowSize) {
            val oldestBucket = ring.removeFirst()
            counts[oldestBucket]--
        }

        val bucket = (durationMillis / bucketWidth).toInt().coerceIn(0, numBuckets - 1)

        ring.addLast(bucket)
        counts[bucket]++
    }

    @Synchronized
    fun count(): Int = ring.size

    @Synchronized
    fun quantile(q: Double): Long {
        require(q in 0.0..1.0)

        if (ring.isEmpty()) {
            return 0L
        }

        val target = ceil(q * ring.size)
            .toInt()
            .coerceAtLeast(1)

        var cumulative = 0

        for (i in counts.indices) {
            cumulative += counts[i]

            if (cumulative >= target) {
                return (i.toLong() + 1L) * bucketWidth
            }
        }

        return numBuckets.toLong() * bucketWidth
    }
}
