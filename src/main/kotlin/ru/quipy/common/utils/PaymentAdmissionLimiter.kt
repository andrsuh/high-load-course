package ru.quipy.common.utils

class PaymentAdmissionLimiter(
    private val ratePerSecond: Int,
    private val maxInFlight: Int = Int.MAX_VALUE,
    private val windowMillis: Long = 1000,
    private val clock: () -> Long = System::nanoTime,
) {
    init { require(ratePerSecond > 0); require(maxInFlight > 0); require(windowMillis > 0) }

    private var inFlight = 0

    @Synchronized
    fun complete() {
        check(inFlight > 0) { "No admitted payment to complete" }
        inFlight--
    }

    private val windowNanos = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(windowMillis)
    private val intervalNanos = (windowNanos + ratePerSecond - 1) / ratePerSecond
    private var nextDispatch: Long? = null

    fun tryAcquire(maxWaitMillis: Long, processingAllowanceMillis: Long = 0): Boolean =
        reserve(maxWaitMillis, processingAllowanceMillis) != null

    @Synchronized
    fun reserve(maxWaitMillis: Long, processingAllowanceMillis: Long = 0): Long? {
        require(processingAllowanceMillis >= 0)
        if (maxWaitMillis < 0 || inFlight >= maxInFlight) return null
        val processingWindows = processingAllowanceMillis / windowMillis +
            if (processingAllowanceMillis % windowMillis == 0L) 0 else 1
        val windows = maxWaitMillis / windowMillis + processingWindows + 1
        val deadlineCapacity = if (windows >= maxInFlight.toLong() / ratePerSecond + 1) {
            maxInFlight.toLong()
        } else {
            minOf(maxInFlight.toLong(), windows * ratePerSecond)
        }
        if (inFlight >= deadlineCapacity) return null
        val now = clock()
        val start = maxOf(now, nextDispatch ?: now)
        val budget = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(maxWaitMillis)
        if (start - now > budget) return null
        nextDispatch = start + intervalNanos
        inFlight++
        return start
    }
}
