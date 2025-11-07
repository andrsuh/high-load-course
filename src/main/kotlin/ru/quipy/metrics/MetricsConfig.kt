package ru.quipy.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("metrics")
class MetricsConfig(
    var retryRequests: MetricProperties = MetricProperties(),
) {
    data class MetricProperties(
        var name: String = "default",
        var description: String = "default",
    )
}