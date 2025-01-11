package ru.quipy

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.data.mongodb.core.MongoTemplate
import ru.quipy.common.utils.NamedThreadFactory
import java.util.*
import java.util.concurrent.Executors


@SpringBootApplication
class OnlineShopApplication {
    val log: Logger = LoggerFactory.getLogger(OnlineShopApplication::class.java)

    companion object {
        val appExecutor = Executors.newFixedThreadPool(64, NamedThreadFactory("main-app-executor"))
    }

    @Bean
    @Throws(Exception::class)
    fun run(mongoTemplate: MongoTemplate): CommandLineRunner? {
        return CommandLineRunner {
            mongoTemplate.db.drop()
        }
    }
}

fun main(args: Array<String>) {
    runApplication<OnlineShopApplication>(*args)
}
