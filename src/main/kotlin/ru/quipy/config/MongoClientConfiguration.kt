package ru.quipy.config

import org.springframework.boot.autoconfigure.mongo.MongoClientSettingsBuilderCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.TimeUnit


@Configuration
class MongoClientConfiguration {

    @Bean
    fun getMongoClientOptionsFactoryBean() = MongoClientSettingsBuilderCustomizer {
        it.applyToClusterSettings {
            it.serverSelectionTimeout(15, TimeUnit.SECONDS)
        }
    }
}