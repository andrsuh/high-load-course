package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl
import java.time.Duration


@Configuration
class ExternalServicesConfig {
    companion object {

        // Ниже приведены готовые конфигурации нескольких аккаунтов провайдера оплаты.
        // Заметьте, что каждый аккаунт обладает своими характеристиками и стоимостью вызова.

        private val accountProps_0 = ExternalServiceProperties(
            "test",
            "default-0",
            parallelRequests = 100000,
            rateLimitPerSec = 1000,
            price = 100, // most expensive. Call costs 100
            averageProcessingTime = Duration.ofMillis(50_000),
            enabled = true,
        )

        private val accountProps_1 = ExternalServiceProperties(
            "test",
            "default-1",
            parallelRequests = 10000,
            rateLimitPerSec = 100,
            price = 100, // most expensive. Call costs 100
            averageProcessingTime = Duration.ofMillis(1000),
            enabled = true,
        )

        private val accountProps_2 = ExternalServiceProperties(
            "test",
            "default-2",
            parallelRequests = 100,
            rateLimitPerSec = 30,
            price = 70, // Call costs 70
            averageProcessingTime = Duration.ofMillis(2_000),
            enabled = true,
        )

        private val accountProps_3 = ExternalServiceProperties(
            "test",
            "default-3",
            parallelRequests = 30,
            rateLimitPerSec = 8,
            price = 40, // Call costs 40
            averageProcessingTime = Duration.ofMillis(3_000),
            enabled = true,
        )

        // Call costs 30
        private val accountProps_4 = ExternalServiceProperties(
            "test",
            "default-4",
            parallelRequests = 8,
            rateLimitPerSec = 5,
            price = 40, // Call costs 40
            averageProcessingTime = Duration.ofMillis(3_000),
            enabled = true,
        )
    }

    @Bean
    fun externalService2() = PaymentExternalSystemAdapterImpl(accountProps_4)
}