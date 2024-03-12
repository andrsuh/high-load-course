package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.time.Duration


@Configuration
class ExternalServicesConfig(
    private val accountBalancer: AccountBalancer
) {
    companion object {
        const val PRIMARY_PAYMENT_BEAN = "PRIMARY_PAYMENT_BEAN"
    }

    @Bean(PRIMARY_PAYMENT_BEAN)
    fun fastExternalService() =
        PaymentExternalServiceImpl(
            accountBalancer
        )
}