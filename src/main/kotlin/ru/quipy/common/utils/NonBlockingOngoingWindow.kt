package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class OngoingWindow(
    maxWinSize: Int,
    fair: Boolean = true,
) {
    private val window = Semaphore(maxWinSize, fair)

    fun acquire() {
        window.acquire()
    }

    fun tryAcquire(timeout: Duration): Boolean {
        return window.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS)
    }

    fun release() = window.release()

    fun awaitingQueueSize() = window.queueLength

    fun isFair() = window.isFair
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