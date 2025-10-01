package ru.quipy.metrics

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

@Component
class HttpMetricsFilter(
    private val metricsCollector: MetricsCollector
) : OncePerRequestFilter() {

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        val accountTag = request.getHeader("X-Account") ?: "unknown"
        metricsCollector.incomingRequestInc(accountTag)
        try {
            filterChain.doFilter(request, response)
        } finally {
            val status = response.status.toString()
            metricsCollector.completedRequestInc(accountTag, status)
        }
    }
}


