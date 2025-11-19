package ru.quipy.common.utils.webclient

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider

@Configuration
class WebClientConfig {
    @Bean
    fun webClient(): WebClient {
        val connectionProvider = ConnectionProvider
            .builder("connection_provider")
            .maxConnections(10_000)
            .build()

        val httpClient = HttpClient.create(connectionProvider)

        return WebClient
            .builder()
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .build()
    }
}