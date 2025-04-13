package ru.quipy.payments.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "rate-limiter")
data class RateLimiterProperties(
    val rate: Long,
    val window: Long,
    val bucketSize: Int,
)
