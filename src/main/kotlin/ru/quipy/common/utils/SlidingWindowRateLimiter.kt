package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {

    init {
        require(rate > 0)
        require(!window.isNegative && !window.isZero)
    }

    private val windowNanos = window.toNanos()

    private val timestamps = ArrayDeque<Long>()

    @Synchronized
    override fun tick(): Boolean {
        val now = System.nanoTime()

        while (
            timestamps.isNotEmpty() &&
            now - timestamps.peekFirst() >= windowNanos
        ) {
            timestamps.removeFirst()
        }

        if (timestamps.size.toLong() >= rate) {
            return false
        }

        timestamps.addLast(now)
        return true
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(5)
        }
    }

    fun tickBlocking(timeout: Duration): Boolean {
        if (timeout.isNegative) {
            return false
        }

        val timeoutNanos = timeout.toNanos()
        val start = System.nanoTime()

        while (true) {
            val remaining =
                timeoutNanos - (System.nanoTime() - start)

            if (remaining < 0L) {
                return false
            }

            if (tick()) {
                return true
            }

            if (remaining == 0L) {
                return false
            }

            val sleepNanos = minOf(
                remaining,
                5_000_000L,
            )

            val millis = sleepNanos / 1_000_000L
            val nanos = (sleepNanos % 1_000_000L).toInt()

            Thread.sleep(millis, nanos)
        }
    }
}