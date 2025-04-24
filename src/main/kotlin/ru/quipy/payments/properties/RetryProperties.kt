package ru.quipy.payments.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "retry")
data class RetryProperties(
    val count: Int,
    val hedgedDelayLimitMs: Long,
)
