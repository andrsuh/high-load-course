package ru.quipy.common.utils

import java.time.Duration
import kotlin.math.sqrt

/**
 * Статистика по последним [capacity] замерам длительности (в мс), не старше [maxAge]: среднее, стандартное отклонение и максимум.
 * Пока таких замеров меньше [minSamples], snapshot() возвращает null.
 *
 * Срок жизни нужен, чтобы один выброс не влиял на решения вечно: если из-за него перестанут принимать оплаты,
 * новых замеров не будет, и без срока жизни максимум не уменьшился бы никогда.
 */
class LatencyStats(
    private val capacity: Int = 1000,
    private val minSamples: Int = 20,
    private val maxAge: Duration = Duration.ofSeconds(30),
) {
    private val samples = LongArray(capacity)
    private val recordedAt = LongArray(capacity)
    private var count = 0
    private var next = 0

    @Synchronized
    fun record(millis: Long) {
        samples[next] = millis
        recordedAt[next] = System.currentTimeMillis()
        next = (next + 1) % capacity
        if (count < capacity) count++
    }

    @Synchronized
    fun snapshot(): Snapshot? {
        val oldest = System.currentTimeMillis() - maxAge.toMillis()
        val window = (0 until count).filter { recordedAt[it] >= oldest }.map { samples[it] }
        if (window.size < minSamples) return null

        val mean = window.average()
        val std = sqrt(window.sumOf { (it - mean) * (it - mean) } / window.size)
        return Snapshot(mean, std, window.max())
    }

    data class Snapshot(val meanMs: Double, val stdMs: Double, val maxMs: Long)
}
