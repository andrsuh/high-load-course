package ru.quipy.config

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.springframework.stereotype.Component

/**
 * Класс для управления метриками платежной системы.
 *
 * Реализует метрики согласно кейсу 5:
 * - Counter: для подсчета событий (запросы, успехи, ошибки)
 * - Gauge: для отслеживания размера очередей и активных запросов
 * - Summary: для анализа времени выполнения запросов с квантилями
 */
@Component
class PaymentMetrics(private val meterRegistry: MeterRegistry) {

    /**
     * Counter метрики - бесконечно растущие счетчики для событий
     */

    // Входящие запросы в платежную систему (до буферизации/очереди)
    fun incrementIncoming(accountName: String) {
        Counter.builder("payment_incoming_requests_total")
            .description("Total number of incoming payment submissions")
            .tag("account", accountName)
            .register(meterRegistry)
            .increment()
    }

    // Исходящие запросы во внешнюю систему (фактические HTTP вызовы)
    fun incrementOutgoing(accountName: String) {
        Counter.builder("payment_outgoing_requests_total")
            .description("Total number of outgoing payment requests to provider")
            .tag("account", accountName)
            .register(meterRegistry)
            .increment()
    }

    // Успешные платежи
    fun incrementSuccess(accountName: String) {
        Counter.builder("payment_success_total")
            .description("Total number of successful payments")
            .tag("account", accountName)
            .register(meterRegistry)
            .increment()
    }

    // Неуспешные платежи
    fun incrementFailure(accountName: String, reason: String) {
        Counter.builder("payment_failures_total")
            .description("Total number of failed payments")
            .tag("account", accountName)
            .tag("reason", sanitizeReason(reason))
            .register(meterRegistry)
            .increment()
    }

    // Повторные попытки (retry)
    fun incrementRetry(accountName: String, statusCode: Int) {
        Counter.builder("payment_retries_total")
            .description("Total number of payment retries")
            .tag("account", accountName)
            .tag("status_code", statusCode.toString())
            .register(meterRegistry)
            .increment()
    }

    // Таймауты
    fun incrementTimeout(accountName: String, timeoutType: String) {
        Counter.builder("payment_timeouts_total")
            .description("Total number of payment timeouts")
            .tag("account", accountName)
            .tag("type", timeoutType)
            .register(meterRegistry)
            .increment()
    }

    fun incrementRejected(accountName: String, reason: String) {
        Counter.builder("payment_rejected_requests_total")
            .description("Total number of payments rejected before dispatch")
            .tag("account", accountName)
            .tag("reason", sanitizeReason(reason))
            .register(meterRegistry)
            .increment()
    }

    /**
     * Gauge метрики - моментальные значения, которые могут расти и убывать
     *
     * Регистрируются через замыкания, которые вызываются при каждом скрейпе Prometheus
     */

    fun registerQueueSizeGauge(accountName: String, queueSizeSupplier: () -> Int) {
        meterRegistry.gauge(
            "payment_queue_size",
            listOf(Tag.of("account", accountName)),
            queueSizeSupplier,
            { it.invoke().toDouble() }
        )
    }

    fun registerBufferQueueGauge(accountName: String, queueSizeSupplier: () -> Int) {
        meterRegistry.gauge(
            "payment_buffer_queue_size",
            listOf(Tag.of("account", accountName)),
            queueSizeSupplier,
            { it.invoke().toDouble() }
        )
    }

    fun registerActiveRequestsGauge(accountName: String, activeRequestsSupplier: () -> Int) {
        meterRegistry.gauge(
            "payment_active_requests",
            listOf(Tag.of("account", accountName)),
            activeRequestsSupplier,
            { it.invoke().toDouble() }
        )
    }

    /**
     * Summary метрики - для измерения времени выполнения с квантилями
     *
     * Summary автоматически вычисляет квантили (0.5, 0.95, 0.99) и sum/count
     */

    fun recordRequestDuration(accountName: String, durationMs: Long) {
        DistributionSummary.builder("payment_request_duration_ms")
            .description("Payment request duration in milliseconds")
            .tag("account", accountName)
            .publishPercentiles(0.5, 0.85, 0.95, 0.99) // медиана, 85%, 95%, 99% квантили
            .register(meterRegistry)
            .record(durationMs.toDouble())
    }

    fun recordQueueWaitTime(accountName: String, waitTimeMs: Long) {
        DistributionSummary.builder("payment_queue_wait_time_ms")
            .description("Time spent waiting in queue in milliseconds")
            .tag("account", accountName)
            .publishPercentiles(0.5, 0.85, 0.95, 0.99)
            .register(meterRegistry)
            .record(waitTimeMs.toDouble())
    }

    // Утилита для очистки reason от специальных символов
    private fun sanitizeReason(reason: String?): String {
        if (reason == null) return "unknown"
        return reason
            .take(50) // ограничиваем длину
            .replace(Regex("[^a-zA-Z0-9_-]"), "_")
            .lowercase()
    }
}
