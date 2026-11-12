package ru.quipy.common.utils

import java.time.Duration

/**
 * Замеры длительности (в мс) с моментом записи. maxOver(horizon) - худший замер среди записанных не раньше чем horizon назад.
 *
 * Горизонт задаёт вызывающий: так статистика всегда отражает недавнее поведение, а один выброс
 * не влияет на решения дольше этого горизонта.
 */
class LatencyStats {
    private val samples = ArrayDeque<Sample>()

    @Synchronized
    fun record(millis: Long) {
        samples.addLast(Sample(System.currentTimeMillis(), millis))
    }

    @Synchronized
    fun maxOver(horizon: Duration): Long? {
        val oldest = System.currentTimeMillis() - horizon.toMillis()
        while (samples.isNotEmpty() && samples.first().recordedAt < oldest) samples.removeFirst()
        return samples.maxOfOrNull { it.millis }
    }

    private data class Sample(val recordedAt: Long, val millis: Long)
}
