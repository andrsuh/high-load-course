package ru.quipy.payments.metrics

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import ru.quipy.common.utils.OngoingWindow
import java.time.Duration

class PaymentAccountMetrics(
    registry: MeterRegistry,
    accountName: String,
    window: OngoingWindow,
) {
    private val windowWaitTimer = Timer.builder("payment.semaphore.wait")
        .tag("account", accountName)
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry)

    init {
        Gauge.builder("payment.external.inflight", window) { it.inFlight().toDouble() }
            .tag("account", accountName)
            .register(registry)

        Gauge.builder("payment.external.waiting", window) { it.awaitingQueueSize().toDouble() }
            .tag("account", accountName)
            .register(registry)
    }

    fun recordWindowWait(duration: Duration) = windowWaitTimer.record(duration)
}
