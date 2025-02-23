package ru.quipy

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.scheduled.ScheduledExecutor
import java.util.concurrent.Executors


@SpringBootApplication
class OnlineShopApplication {
    val log: Logger = LoggerFactory.getLogger(OnlineShopApplication::class.java)

    @Autowired
    private lateinit var scheduledExecutor: ScheduledExecutor

    companion object {
        val appExecutor = Executors.newFixedThreadPool(64, NamedThreadFactory("main-app-executor"))
    }
}

fun main(args: Array<String>) {
    runApplication<OnlineShopApplication>(*args)
}
