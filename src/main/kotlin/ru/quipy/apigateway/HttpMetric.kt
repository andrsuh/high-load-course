package ru.quipy.apigateway

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import io.micrometer.core.instrument.Gauge
import java.util.concurrent.atomic.AtomicLong

@Component
class HttpMetrics() {

    val requestsCounter = Counter.builder("pay_requests_total")
            .description("Total number of paymennt requests")
            .register(Metrics.globalRegistry)

val requestsCounter2 = Counter.builder("pay_requests_total_after_rate_limiter")
            .description("Total number of paymennt requests ater rate limiter")
            .register(Metrics.globalRegistry)


    val responceCounter = Counter.builder("pay_requests_completed")
            .description("Total number of completed payment requests")
            .register(Metrics.globalRegistry)

    val toManyRespCounter = Counter.builder("to_many_responce")
            .description("Total number of to many responce after 1 check")
            .tag("check", "1")
            .register(Metrics.globalRegistry)

val toManyRespCounter2 = Counter.builder("to_many_responce")
            .description("Total number of to many responce  after 2 check")
            .tag("check", "2")
            .register(Metrics.globalRegistry)

    val toManyRequestsDelayTime: Timer = Timer.builder("to_many_duration_seconds")
            .description("rime of delay requests after 1 check")
             .tag("check", "1")
            .register(Metrics.globalRegistry)

 val toManyRequestsDelayTime2: Timer = Timer.builder("to_many_duration_seconds")
            .description("rime of delay requests after 2 check")
             .tag("check", "2")
            .register(Metrics.globalRegistry)


        val inQueueCount = AtomicLong(0)


        val allQueueCounter: Gauge = Gauge.builder(
        "requests_in_queue_total",
        java.util.function.Supplier { inQueueCount.get() }
    )
        .description("Total number of payment requests in queue")
        .tag("queue", "all")
        .register(Metrics.globalRegistry)


}