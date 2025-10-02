package ru.quipy.common.utils

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger
import java.time.Duration
import java.util.concurrent.TimeUnit

class OngoingWindow(
    maxWinSize: Int
) {
    private val window = Semaphore(maxWinSize)

    fun acquire() {
        window.acquire()
    }

    fun acquire(timeout: Duration): Boolean {
        return window.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS)
    }

    fun release() = window.release()

    fun awaitingQueueSize() = window.queueLength
}

class NonBlockingOngoingWindow(
    private val maxWinSize: Int
) {
    private val winSize = AtomicInteger()

    fun putIntoWindow(): WindowResponse {
        while (true) {
            val currentWinSize = winSize.get()
            if (currentWinSize >= maxWinSize) {
                return WindowResponse.Fail(currentWinSize)
            }

            if (winSize.compareAndSet(currentWinSize, currentWinSize + 1)) {
                break
            }
        }
        return WindowResponse.Success(winSize.get())
    }

    fun putIntoWindow(timeout: Duration): Boolean {
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        while (System.currentTimeMillis() < deadline) {
            val response = putIntoWindow()
            if (response is WindowResponse.Success) {
                return true
            }
            Thread.sleep(1)
        }
        return false
    }

    fun releaseWindow() = winSize.decrementAndGet()


    sealed class WindowResponse(val currentWinSize: Int) {
        public class Success(
            currentWinSize: Int
        ) : WindowResponse(currentWinSize)

        public class Fail(
            currentWinSize: Int
        ) : WindowResponse(currentWinSize)
    }
}