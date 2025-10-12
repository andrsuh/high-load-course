package ru.quipy.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import org.springframework.stereotype.Component

@Component
class MetricsCollector {
    fun incomingRequestInc() {
        Counter
            .builder("incoming_requests_total")
            .description("Number of incoming requests")
            .register(Metrics.globalRegistry)
            .increment()
    }

    fun successfulRequestInc(account: String) {
        Counter
            .builder("successful_requests_total")
            .description("Number of successful requests")
            .tags("account", account)
            .register(Metrics.globalRegistry)
            .increment()
    }

    fun failedRequestInc(account: String) {
        Counter
            .builder("failed_requests_total")
            .description("Number of failed requests")
            .tags("account", account)
            .register(Metrics.globalRegistry)
            .increment()
    }

    fun failedRequestExternalInc(account: String) {
        Counter
            .builder("failed_requests_external_total")
            .description("Number of failed external requests")
            .tags("account", account)
            .register(Metrics.globalRegistry)
            .increment()
    }
}