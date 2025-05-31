package ru.quipy.common.utils

import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class OngoingWindow(
    maxWinSize: Int
) {
    private val window = Semaphore(maxWinSize)

    fun acquire() {
        window.acquire()
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

        fun isSuccess() = this is Success
    }
}

/**
 * Первая попытка добавления неблокирующая. Если окно заполнено,
 * можно ожидать освобождения места в течение указанного дедлайна.
 */
class NonBlockingDeadlineWindow(
    private val maxWinSize: Int
) {
    private val winSize = AtomicInteger(0)

    // Инициализирован 0 пермитами, т.к. пермит выдается только при освобождении окна (releaseWindow),
    // и только если есть ожидающие потоки.
    private val slotAvailableSignal = Semaphore(0, true)

    /**
     * Пытается поместить элемент в окно.
     * Сначала выполняется неблокирующая попытка. Если окно заполнено и deadlineMs > 0,
     * поток будет ожидать освобождения места до истечения дедлайна.
     */
    fun putIntoWindow(deadlineMs: Long): WindowResponse {
        var remainingNanos = if (deadlineMs > 0) TimeUnit.MILLISECONDS.toNanos(deadlineMs) else 0L

        while (true) {
            val currentLocalWinSize = winSize.get()

            if (currentLocalWinSize < maxWinSize) {
                if (winSize.compareAndSet(currentLocalWinSize, currentLocalWinSize + 1)) {
                    return WindowResponse.Success(currentLocalWinSize + 1)
                }
                continue
            }

            if (remainingNanos <= 0) {
                return WindowResponse.Fail(currentLocalWinSize)
            }

            val waitStartTimeNanos = System.nanoTime()
            try {
                if (slotAvailableSignal.tryAcquire(remainingNanos, TimeUnit.NANOSECONDS)) {
                    remainingNanos -= (System.nanoTime() - waitStartTimeNanos)
                    continue
                } else {
                    return WindowResponse.Fail(winSize.get())
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return WindowResponse.Fail(winSize.get())
            }
        }
    }

    /**
     * Освобождает один слот в окне.
     * Уменьшает счетчик активных элементов и, если есть ожидающие потоки,
     * сигнализирует одному из них, что слот мог освободиться.
     */
    fun releaseWindow(): Int {
        val newSize = winSize.decrementAndGet()

        if (slotAvailableSignal.hasQueuedThreads()) {
            slotAvailableSignal.release()
        }
        return newSize
    }

    sealed class WindowResponse(val currentWinSizeReported: Int) {
        class Success(
            currentWinSizeAfterIncrement: Int
        ) : WindowResponse(currentWinSizeAfterIncrement)

        class Fail(
            currentWinSizeAtFailure: Int
        ) : WindowResponse(currentWinSizeAtFailure)

        fun isSuccess() = this is Success
        fun isFail() = this is Fail // Удобный метод для проверки неудачи
    }
}
