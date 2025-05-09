package ru.quipy.common.utils

import kotlinx.coroutines.*
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock
import kotlin.coroutines.CoroutineContext
import kotlin.math.min
import java.util.concurrent.atomic.AtomicInteger

class RateAndConcurrencyLimiter(
    private val rateLimit: Int,
    private val rateLimitWindowMs: Long,
    private val concurrencyLimit: Long,
) {
    private val tickTimestamps = LongArray(rateLimit)
    private val tickTimestampsRwLock = ReentrantReadWriteLock()
    private var head = 0

//    private var concurrencyCount : Long = 0
//    private var concurrencyRwLock = ReentrantReadWriteLock()
    private val concurrencySemaphore = Semaphore(concurrencyLimit.toInt())

    private var blockingLoopDelayMs = min(2, rateLimitWindowMs / concurrencyLimit / 2)

    fun executeWithDeadlineAsync(coroutineCtx: CoroutineContext, deadline: Long, block: () -> Unit) {
        CoroutineScope(coroutineCtx).launch {
            withTimeout(deadline - System.currentTimeMillis()) {
                execute(block)
            }
        }
    }

    suspend fun execute(block: () -> Unit) {
        lease()
        try {
            block()
        } finally {
            release()
        }
    }

    suspend fun lease() {
        while (true) {
            concurrencySemaphore.acquire()
            if (!addRate()) {
                concurrencySemaphore.release()
                continue
            }

            return
        }
    }

    fun release() {
        concurrencySemaphore.release()
//        concurrencyCount.decrementAndGet()
//        concurrencyRwLock.writeLock().withLock {
//            concurrencyCount--
//        }
    }

//    private fun canAddConcurrent(): Boolean {
//        return concurrencyCount.get() < concurrencyLimit
////        concurrencyRwLock.readLock().withLock {
////            var res = concurrencyCount < concurrencyLimit
////            if (!res) {
//////                println("can't add concurrent")
////            }
////            return res
////        }
//    }

//    private fun addConcurrent(): Boolean {
//        if (concurrencyCount.get() < concurrencyLimit) {
//            concurrencyCount.incrementAndGet()
//            return true
//        }
//        return false
////        concurrencyRwLock.writeLock().withLock {
////            if (concurrencyCount >= concurrencyLimit) {
//////                println("could not add concurrent")
////                return false
////            }
////            concurrencyCount++
////            return true
////        }
//    }

    private fun canAddRate(): Boolean {
        val now = System.currentTimeMillis()
        val windowStart = now - rateLimitWindowMs
        tickTimestampsRwLock.readLock().withLock {
            if (tickTimestamps[head] < windowStart) {
                return true
            }
//            logRateLimitHit(now)
            return false
        }
    }

    private fun addRate(): Boolean {
        val now = System.currentTimeMillis()
        val windowStart = now - rateLimitWindowMs
        tickTimestampsRwLock.writeLock().withLock {
            if (tickTimestamps[head] < windowStart) {
                tickTimestamps[head] = now
                head = (head + 1) % rateLimit
                return true
            }
//            println("could not add rate")
            return false
        }
    }

    private fun logRateLimitHit(now: Long) {
        val formatter = DateTimeFormatter
            .ofPattern("mm:ss.SSS")
            .withZone(ZoneId.systemDefault())

        val nowFormatted = formatter.format(Instant.ofEpochMilli(now))
        println("can't add rate at ${nowFormatted}, window: ${formatTicks(formatter)}")
    }

    private fun formatTicks(formatter: DateTimeFormatter): String {
        val result = mutableListOf<String>()
        var i = head
        do {
            result.add(formatter.format(Instant.ofEpochMilli(tickTimestamps[i])))
            i = (i + 1) % tickTimestamps.size
        } while (i != head)

        return result.joinToString(prefix = "[", postfix = "]")
    }
}