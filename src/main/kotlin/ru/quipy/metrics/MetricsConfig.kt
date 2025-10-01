package ru.quipy.metrics

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.client.RestTemplate

@Configuration
class MetricsConfig(
    private val outgoingMetricsInterceptor: OutgoingMetricsInterceptor
) {
    @Bean
    fun restTemplate(): RestTemplate {
        val rt = RestTemplate()
        val interceptors = ArrayList(rt.interceptors)
        interceptors.add(outgoingMetricsInterceptor)
        rt.interceptors = interceptors
        return rt
    }
}
