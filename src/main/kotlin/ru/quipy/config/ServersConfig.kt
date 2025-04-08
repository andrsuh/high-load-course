package ru.quipy.config

import org.apache.catalina.startup.Tomcat
import org.apache.coyote.http2.Http2Protocol
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.eclipse.jetty.util.Jetty
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class Http2Config {
    // Конфигурация будет применяться только если используется Tomcat
    @ConditionalOnClass(Tomcat::class)
    @Bean
    fun tomcatConnectorCustomizer(): TomcatConnectorCustomizer {
        return TomcatConnectorCustomizer { connector ->
            val http2Protocol = connector.protocolHandler.findUpgradeProtocols()
                .firstOrNull { it is Http2Protocol } as? Http2Protocol
                ?: return@TomcatConnectorCustomizer

            http2Protocol.maxConcurrentStreams = 10_000_000
            logger.info("Configured Tomcat HTTP/2 with maxConcurrentStreams=10,000,000")
        }
    }

    // Конфигурация будет применяться только если используется Jetty
    @ConditionalOnClass(Jetty::class)
    @Bean
    fun jettyServerCustomizer(): JettyServletWebServerFactory {
        return JettyServletWebServerFactory().apply {
            addServerCustomizers(JettyServerCustomizer { server ->
                server.connectors.forEach { connector ->
                    (connector.getConnectionFactory("h2c") as? HTTP2CServerConnectionFactory)?.let {
                        it.maxConcurrentStreams = 1_000_000
                        logger.info("Configured Jetty HTTP/2 with maxConcurrentStreams=1,000,000")
                    }
                }
            })
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Http2Config::class.java)
    }
}