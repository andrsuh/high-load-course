package ru.quipy.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import org.springframework.stereotype.Component

@Component
class MetricsCollector {
    fun incomingRequestInc(account: String) {
        Counter
            .builder("incoming_requests_total")
            .description("Number of incoming requests")
            .tags("account", account)
            .register(Metrics.globalRegistry)
            .increment()
    }

    fun completedRequestInc(account: String, status: String) {
        Counter
            .builder("completed_requests_total")
            .description("Number of completed requests")
            .tags("account", account, "status", status)
            .register(Metrics.globalRegistry)
            .increment()
    }

    fun outgoingRequestInc(target: String) {
        Counter
            .builder("outgoing_requests_total")
            .description("Number of outgoing requests")
            .tags("target", target)
            .register(Metrics.globalRegistry)
            .increment()
    }
}