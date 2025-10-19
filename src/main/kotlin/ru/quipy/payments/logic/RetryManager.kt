package ru.quipy.payments.logic

import kotlin.math.pow
import kotlin.random.Random

class RetryManager(
    private val maxRetries: Int,
    private val backoffFactor: Double = 2.0,
    private val jitterMillis: Long = 50L,
    private val avgProcessingTime: Long = 1000L
) {
    private var attempt = 0
    private var delays: LongArray? = null
    private var deadline: Long = 0
    private var startTime: Long = 0

    fun shouldRetry(currentTime: Long, deadline: Long): Boolean {
        if (currentTime >= deadline) return false
        if (attempt == maxRetries) return false
        if (attempt == 0) {
            this.deadline = deadline
            this.startTime = currentTime
            computeDelays()
        }
        return true
    }

    private fun computeDelays() {
        val availableTime = deadline - startTime - avgProcessingTime
        if (availableTime <= 0) {
            delays = LongArray(maxRetries) { 0 }
            attempt = maxRetries
            return
        }

        val sumFactor = (backoffFactor.pow(maxRetries - 1) - 1) / (backoffFactor - 1)
        val baseDelay = availableTime / sumFactor
        delays = LongArray(maxRetries) { i ->
            if (i == maxRetries - 1) 0
            else (baseDelay * backoffFactor.pow(i.toDouble())).toLong()
        }
    }

    fun onFailure() {
        attempt++
        val delay = if (attempt < maxRetries) {
            delays?.getOrNull(attempt - 1) ?: 0
        } else {
            0
        }
        val jitter = Random.nextLong(0, jitterMillis + 1)
        val totalDelay = delay + jitter + startTime - System.currentTimeMillis()
        if (totalDelay > 0) {
            Thread.sleep(totalDelay)
        }
    }
}
