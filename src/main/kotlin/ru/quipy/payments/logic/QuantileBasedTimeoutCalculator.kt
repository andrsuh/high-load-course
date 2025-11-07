package ru.quipy.payments.logic

import java.time.Duration
import java.util.*
import kotlin.math.min

class AdaptiveTimeoutCalculator(
    private val targetQuantile: Double = 0.85,
    private val quantileMargin: Double = 0.30,
    private val minTimeout: Duration = Duration.ofMillis(800),
    private val maxTimeout: Duration = Duration.ofSeconds(8)
) {
    private val latencyMeasurements = Collections.synchronizedList(mutableListOf<Long>())
    private var requestCount = 0
    private val updateThreshold = 25

    fun addLatencyMeasurement(latencyMs: Long) {
        synchronized(latencyMeasurements) {
            latencyMeasurements.add(latencyMs)
            requestCount++
            if (latencyMeasurements.size > 500) {
                latencyMeasurements.removeFirst()
            }
        }
    }

    fun shouldUpdateTimeout(): Boolean {
        return requestCount % updateThreshold == 0
    }

    fun calculateOptimalTimeout(remainingBudget: Long? = null): Duration {
        synchronized(latencyMeasurements) {
            if (latencyMeasurements.isEmpty()) {
                return if (remainingBudget != null) {
                    Duration.ofMillis(minOf(remainingBudget / 2, 2000))
                } else {
                    Duration.ofMillis(1200)
                }
            }

            val sortedLatencies = latencyMeasurements.sorted()
            val n = sortedLatencies.size

            val referenceQuantile = calculateQuantile(sortedLatencies, n, 0.75)
            val targetQuantileValue = calculateQuantile(sortedLatencies, n, targetQuantile)

            val maxAllowed = referenceQuantile * (1 + quantileMargin)
            var optimalTimeout = minOf(targetQuantileValue, maxAllowed)

            if (remainingBudget != null) {
                optimalTimeout = minOf(optimalTimeout, remainingBudget * 0.8)
            }

            val boundedTimeout = optimalTimeout.coerceIn(
                minTimeout.toMillis().toDouble(),
                maxTimeout.toMillis().toDouble()
            )

            return Duration.ofMillis(boundedTimeout.toLong())
        }
    }

    private fun calculateQuantile(sortedData: List<Long>, n: Int, quantile: Double): Double {
        if (n == 0) return 0.0
        val pos = quantile * (n - 1)
        val lowerIndex = pos.toInt()
        val fraction = pos - lowerIndex

        return if (lowerIndex >= n - 1) {
            sortedData.last().toDouble()
        } else {
            sortedData[lowerIndex] * (1 - fraction) + sortedData[lowerIndex + 1] * fraction
        }
    }
}