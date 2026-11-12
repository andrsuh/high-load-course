package ru.quipy.common.utils

import java.time.Duration
import kotlin.math.sqrt

/**
 * Замеры длительности (в мс) с моментом записи. maxOver / meanOver / stdOver(horizon) - худший, средний замер и разброс
 * среди записанных не раньше чем horizon назад.
 *
 * Горизонт задаёт вызывающий: так статистика всегда отражает недавнее поведение, а один выброс
 * не влияет на решения дольше этого горизонта.
 */
class LatencyStats {
    private val samples = ArrayDeque<Sample>()
    private var horizon = Duration.ZERO

    @Synchronized
    fun record(millis: Long) {
        samples.addLast(Sample(System.currentTimeMillis(), millis))
        dropOlderThan(horizon)
    }

    @Synchronized
    fun maxOver(horizon: Duration): Long? {
        dropOlderThan(horizon)
        return samples.maxOfOrNull { it.millis }
    }

    @Synchronized
    fun meanOver(horizon: Duration): Double? {
        dropOlderThan(horizon)
        return if (samples.isEmpty()) null else samples.sumOf { it.millis }.toDouble() / samples.size
    }

    @Synchronized
    fun stdOver(horizon: Duration): Double? {
        val mean = meanOver(horizon) ?: return null
        return sqrt(samples.sumOf { (it.millis - mean) * (it.millis - mean) } / samples.size)
    }

    // Храним только замеры в пределах последнего запрошенного горизонта
    private fun dropOlderThan(horizon: Duration) {
        this.horizon = horizon
        val oldest = System.currentTimeMillis() - horizon.toMillis()
        while (samples.isNotEmpty() && samples.first().recordedAt < oldest) samples.removeFirst()
    }

    private data class Sample(val recordedAt: Long, val millis: Long)
}
