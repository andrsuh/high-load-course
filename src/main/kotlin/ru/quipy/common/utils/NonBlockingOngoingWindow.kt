package ru.quipy.common.utils

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit

class OngoingWindow(
    maxWinSize: Int
) {
    private val window = Semaphore(maxWinSize, true)

    fun acquire() {
        window.acquire()
    }

    fun tryAcquire(deadline: Long): Boolean {
        return window.tryAcquire(deadline, TimeUnit.MILLISECONDS)
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