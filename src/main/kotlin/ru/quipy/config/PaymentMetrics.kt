package ru.quipy.config

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class PaymentMetrics(private val registry: MeterRegistry) {
    private val incomingRequests = registry.counter("success_requests")
    private val failedOutgoingRequests = registry.counter("failed_outgoing_requests")
    private val successOutgoingRequests = registry.counter("success_outgoing_requests")

    fun incomingRequests() = incomingRequests.increment()
    fun successOutgoingRequests() = successOutgoingRequests.increment()
    fun failedOutgoingRequests() = failedOutgoingRequests.increment()
}