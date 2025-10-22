package ru.quipy.apigateway

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component

@Component
class HttpMetrics() {

    val requestsCounter = Counter.builder("pay_requests_total")
            .description("Total number of paymennt requests")
            .register(Metrics.globalRegistry)


    val responceCounter = Counter.builder("pay_requests_completed")
            .description("Total number of completed payment requests")
            .register(Metrics.globalRegistry)

    val toManyRespCounter = Counter.builder("to_many_responce")
            .description("Total number of to many responce")
            .register(Metrics.globalRegistry)

    val toManyRequestsDelayTime: Timer = Timer.builder("to_many_duration_seconds")
            .description("rime of delay requests")
            .register(Metrics.globalRegistry)


}