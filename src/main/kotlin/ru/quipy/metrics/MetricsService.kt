package ru.quipy.metrics

import org.springframework.stereotype.Service
import ru.quipy.config.MetricsConfig
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Tag

@Service
class MetricsService(
    val metricsConfig: MetricsConfig,
) {
    fun increaseRetryCounter() {
        writeCounter(metricsConfig.retryRequests).increment()
    }

    fun writeCounter(config: MetricsConfig.MetricProperties) = Counter
        .builder(config.name)
        .description(config.description)
        .register(Metrics.globalRegistry)
}