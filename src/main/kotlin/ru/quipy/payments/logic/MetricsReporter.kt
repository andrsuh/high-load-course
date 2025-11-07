package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

@Component
class MetricsReporter(private val meterRegistry: MeterRegistry) {

    private val retryCounter = Counter.builder("payment.requests.retries")
        .description("Total retry attempts for payment requests")
        .register(meterRegistry)

    private val timeoutGaugesByAccount = ConcurrentHashMap<String, AtomicLong>()

    fun incrementRetry() {
        retryCounter.increment()
    }

    fun updateCurrentTimeout(account: String, timeoutMs: Long) {
        val holder = timeoutGaugesByAccount.computeIfAbsent(account) { acc ->
            val atomic = AtomicLong(timeoutMs)
            Gauge.builder("payment.timeout.current_ms", atomic) { it.get().toDouble() }
                .description("Current read timeout used for external payment requests")
                .tag("account", acc)
                .register(meterRegistry)
            atomic
        }
        holder.set(timeoutMs)
    }
}