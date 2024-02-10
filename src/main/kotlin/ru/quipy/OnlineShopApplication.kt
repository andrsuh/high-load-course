package ru.quipy

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.data.mongodb.core.MongoTemplate
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.delivery.api.DeliveryAggregate
import ru.quipy.delivery.logic.DeliveryAggregateState
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.logic.OrderAggregateState
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.warehouse.api.BookingAggregate
import ru.quipy.warehouse.api.ProductAggregate
import ru.quipy.warehouse.logic.BookingAggregateState
import ru.quipy.warehouse.logic.ProductAggregateState
import ru.quipy.warehouse.logic.create
import java.util.*
import java.util.concurrent.Executors


@SpringBootApplication
class OnlineShopApplication {
    val log: Logger = LoggerFactory.getLogger(OnlineShopApplication::class.java)

    companion object {
        val appExecutor = Executors.newFixedThreadPool(64, NamedThreadFactory("main-app-executor"))
    }

    @Autowired
    private lateinit var productESService: EventSourcingService<UUID, ProductAggregate, ProductAggregateState>

    @Autowired
    private lateinit var bookingESService: EventSourcingService<UUID, BookingAggregate, BookingAggregateState>

    @Autowired
    private lateinit var ordersESService: EventSourcingService<UUID, OrderAggregate, OrderAggregateState>

    @Autowired
    private lateinit var deliveryESService: EventSourcingService<UUID, DeliveryAggregate, DeliveryAggregateState>

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Bean
    @Throws(Exception::class)
    fun run(mongoTemplate: MongoTemplate): CommandLineRunner? {
        return CommandLineRunner { args: Array<String?>? ->
            mongoTemplate.db.drop()

            val productsIds = mutableListOf<UUID>()

            productESService.create {
                val product1Id = UUID.randomUUID().also { productsIds.add(it) }
                it.create(product1Id, "Product 1", 100)
            }
            productESService.create {
                val product2Id = UUID.randomUUID().also { productsIds.add(it) }
                it.create(product2Id, "Product 2", 200)
            }
            productESService.create {
                val product3Id = UUID.randomUUID().also { productsIds.add(it) }
                it.create(product3Id, "Product 3", 300)
            }

//            Thread.sleep(5_000)
//
//            val orderCreatedEvent = ordersESService.create {
//                it.create(UUID.randomUUID())
//            }
//
//            (1..4).forEach {
//                ordersESService.updateSerial(orderCreatedEvent.orderId) {
//                    it.addItem(productsIds.random(), nextInt(6))
//                }
//            }
//
//            ordersESService.update(orderCreatedEvent.orderId) {
//                it.checkout()
//            }
//
//            Thread.sleep(10_000)
//
//            deliveryESService.create {
//                it.create(UUID.randomUUID(), orderCreatedEvent.orderId, 100_000)
//            }
//
//            Thread.sleep(3_000)
//
//            ordersESService.update(orderCreatedEvent.orderId) {
//                it.startPayment()
//            }
//
//            Thread.sleep(10_000)

//            for (i in 0..3) {
//                bookingESService
//                    .create {
//                        it.create(
//                            UUID.randomUUID(),
//                            UUID.randomUUID(),
//                            (1..3).map { BookedProduct(productsIds.random(), nextInt(11)) })
//                    }
//            }
//        }
//    }
        }
    }
}

fun main(args: Array<String>) {
    runApplication<OnlineShopApplication>(*args)
}
