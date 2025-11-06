package ru.quipy.payments.api

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit

@Component
class PaymentMetric {
    val incomingRequestsCounter = Counter.builder("payment_requests_incoming_total")
        .description("Total number of incoming payment requests")
        .register(Metrics.globalRegistry)


    val paymentResponceCounter = Counter.builder("payment_responce_total")
        .description("Total number of payment responce")
        .register(Metrics.globalRegistry)

    val paymentRetryCounter = Counter.builder("retry_total")
        .description("Total number of payment retries")
        .register(Metrics.globalRegistry)

    val rateLimiterQueueCount = AtomicLong(0)
    val semaphoreQueueCount = AtomicLong(0)


    val rateLimiterQueueCounter: Gauge = Gauge.builder(
        "requests_in_queue_total",
        java.util.function.Supplier { rateLimiterQueueCount.get() }
    )
        .description("Total number of payment requests in queue")
        .tag("queue", "rate limiter")
        .register(Metrics.globalRegistry)

    val semaphoreQueueCounter: Gauge = Gauge.builder(
        "requests_in_queue_total",
        java.util.function.Supplier { semaphoreQueueCount.get() }
    )
        .description("Total number of payment requests in queue")
        .tag("queue", "semaphore")
        .register(Metrics.globalRegistry)


    val rateLimiterQueueDurationTimer: Timer = Timer.builder("in_queue_duration_seconds")
        .description("Waiting in queue in seconds")
        .tag("queue", "rate limiter")
        .register(Metrics.globalRegistry)

     val semaphoreQueueDurationTimer: Timer = Timer.builder("in_queue_duration_seconds")
        .description("Waiting in queue in seconds")
        .tag("queue", "semaphore")
        .register(Metrics.globalRegistry)

    private val callTimerBuilder: Timer.Builder = Timer.builder("call_time_milliseconds")
        .description("Call duration in milliseconds")
        .publishPercentiles(0.5, 0.9, 0.99)
        .publishPercentileHistogram()

    fun recordLatency(statusCode: Int, durationMillis: Long) {
        val timer = callTimerBuilder
            .tag("status_code", statusCode.toString())
            .register(Metrics.globalRegistry)

        timer.record(durationMillis, TimeUnit.MILLISECONDS)
    }
}
