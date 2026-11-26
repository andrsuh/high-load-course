package ru.quipy.payments.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "payment")
class PaymentTimeoutProperties {
    val timeout: MutableMap<String, Duration> = mutableMapOf()
    var maxAttempts: Int = 4
    var baseDelayMs: Long = 200
    var maxDelayMs: Long = 800
}
