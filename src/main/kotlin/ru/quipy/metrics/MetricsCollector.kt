package ru.quipy.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Gauge
import org.springframework.stereotype.Component
import java.util.concurrent.LinkedBlockingQueue

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

    fun status429RequestInc() {
        Counter
            .builder("too_many_requests")
            .description("Number of failed requests")
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

    fun requestsQueueSizeRegister(linkedBlockingQueue: LinkedBlockingQueue<Runnable>) {
        Gauge
            .builder("queue_size", linkedBlockingQueue) { it.size.toDouble() }
                .register(Metrics.globalRegistry)
    }
}