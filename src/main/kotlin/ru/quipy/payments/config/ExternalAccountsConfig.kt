package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse


@Configuration
class ExternalAccountsConfig {
    companion object {
        private val PAYMENT_PROVIDER_HOST_PORT: String = "localhost:1234"
        private val javaClient = HttpClient.newBuilder().build()
        private val mapper = ObjectMapper().registerKotlinModule().registerModules(JavaTimeModule())
    }

    private val allowedAccounts = setOf("test-account")

    @Bean
    fun accountAdapters(): List<PaymentExternalSystemAdapter> {
        val request = HttpRequest.newBuilder()
            .uri(URI("http://${PAYMENT_PROVIDER_HOST_PORT}/external/accounts?serviceName=test")) // todo sukhoa service name
            .GET()
            .build()

        val resp = javaClient.send(request, HttpResponse.BodyHandlers.ofString())
        return mapper.readValue<List<ExternalServiceProperties>>(
            resp.body(),
            mapper.typeFactory.constructCollectionType(List::class.java, ExternalServiceProperties::class.java)
        )
            .filter {
                it.accountName in allowedAccounts
            }.onEach(::println)
            .map { PaymentExternalSystemAdapterImpl(it) }
    }
}