package ru.quipy.config

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class PaymentMetrics(private val registry: MeterRegistry) {
    private val incomingRequests = registry.counter("incoming_requests")
    private val failedIncomingRequests = registry.counter("failed_incoming_requests")
    private val failedOutgoingRequests = registry.counter("failed_outgoing_requests")
    private val outgoingRequests = registry.counter("outgoing_requests")

    fun incomingRequests() = incomingRequests.increment()
    fun failedIncomingRequests() = failedIncomingRequests.increment()
    fun outgoingRequests() = outgoingRequests.increment()
    fun failedOutgoingRequests() = failedOutgoingRequests.increment()
}