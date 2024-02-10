package ru.quipy.config

import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration


@Configuration
open class MongoConfig : AbstractMongoClientConfiguration() {
    override fun getDatabaseName(): String {
        return "online-shop"
    }

    public override fun autoIndexCreation(): Boolean {
        return true
    }
}