package ru.quipy.common.utils

import java.time.Duration

/**
 * Скользящее окно с бронированием: выдаёт моменты отправки так, чтобы в любом отрезке длиной window
 * было не больше [permits] отправок. Длину окна передаёт вызывающий: она может меняться по замерам.
 * Моменты выдаются по порядку (FIFO): каждый следующий не раньше предыдущего.
 *
 * В отличие от обычного rate limiter, запрос получает не «да / нет прямо сейчас», а ближайший момент,
 * когда для него будет разрешение, и может сам решить, устраивает ли его такое ожидание.
 */
class SlidingWindowReservations(
    private val permits: Int,
) {
    // Последние [permits] выданных моментов, кольцом: slots[oldest] - самый ранний из них
    private val slots = LongArray(permits)
    private var oldest = 0
    private var last = 0L

    /**
     * Ближайший момент, который получит следующая бронь.
     */
    @Synchronized
    fun nextSlot(now: Long, window: Duration): Long = maxOf(now, last, slots[oldest] + window.toMillis())

    /**
     * Бронирует ближайший момент, если [accept] на него согласен, и возвращает его; иначе null.
     */
    @Synchronized
    fun reserve(now: Long, window: Duration, accept: (Long) -> Boolean): Long? {
        val slot = nextSlot(now, window)
        if (!accept(slot)) return null

        slots[oldest] = slot
        oldest = (oldest + 1) % permits
        last = slot
        return slot
    }
}
