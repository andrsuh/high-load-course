package ru.quipy.metrics

import org.springframework.http.HttpRequest
import org.springframework.http.client.ClientHttpRequestExecution
import org.springframework.http.client.ClientHttpRequestInterceptor
import org.springframework.http.client.ClientHttpResponse
import org.springframework.stereotype.Component

@Component
class OutgoingMetricsInterceptor(
    private val metricsCollector: MetricsCollector
) : ClientHttpRequestInterceptor {

    override fun intercept(
        request: HttpRequest,
        body: ByteArray,
        execution: ClientHttpRequestExecution
    ): ClientHttpResponse {
        val target = request.uri.host ?: "unknown"
        metricsCollector.outgoingRequestInc(target)
        return execution.execute(request, body)
    }
}


