package ru.quipy.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl

@Configuration
class RateConfiguration {

    @Bean
    fun apiRateLimit(accountAdapters: List<PaymentExternalSystemAdapter>): Long {
        return accountAdapters
            .filterIsInstance<PaymentExternalSystemAdapterImpl>()
            .sumOf { it.getRateLimitPerSec().toLong() }
    }
}

