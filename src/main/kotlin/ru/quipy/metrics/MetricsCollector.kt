package ru.quipy.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import org.springframework.stereotype.Component

@Component
class MetricsCollector {
    fun incomingRequestInc(account: String) = Counter
        .builder("incoming_request")
        .description("Number of incoming requests")
        .tags("account", account)
        .register(Metrics.globalRegistry)
        .increment();
}