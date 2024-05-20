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

        private val accountProps_1 = ExternalServiceProperties(
            "test",
            "default-1",
            parallelRequests = 2000,
            rateLimitPerSec = 100,
            price = 100, // most expensive. Call costs 100
            averageProcessingTime = Duration.ofMillis(40_000),
            enabled = false,
        )

        private val accountProps_2 = ExternalServiceProperties(
            "test",
            "default-2",
            parallelRequests = 100,
            rateLimitPerSec = 10,
            price = 70, // Call costs 70
            enabled = false,
        )

        private val accountProps_3 = ExternalServiceProperties(
            "test",
            "default-3",
            parallelRequests = 32,
            rateLimitPerSec = 2,
            price = 40, // Call costs 40
            averageProcessingTime = Duration.ofMillis(16_000),
            enabled = false,
        )

        // Call costs 30
        private val accountProps_4 = ExternalServiceProperties(
            "test",
            "default-4",
            parallelRequests = 15,
            rateLimitPerSec = 5,
            price = 40, // Call costs 40
            averageProcessingTime = Duration.ofMillis(15_000),
            enabled = true,
        )

        private val accountProps_5 = ExternalServiceProperties(
            "test",
            "default-5",
            parallelRequests = 8,
            rateLimitPerSec = 10,
            price = 40, // Call costs 40
            averageProcessingTime = Duration.ofMillis(10_000),
            enabled = false,
        )

        private val accountProps_6 = ExternalServiceProperties(
            "test",
            "default-6",
            parallelRequests = 35,
            rateLimitPerSec = 30,
            price = 40, // Call costs 40
            averageProcessingTime = Duration.ofMillis(1_000),
            enabled = false,
        )
    }

    @Bean
    fun externalService2() = PaymentExternalSystemAdapterImpl(accountProps_4)
}