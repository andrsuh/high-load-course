package ru.quipy.warehouse.projections

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import ru.quipy.OnlineShopApplication.Companion.appExecutor
import ru.quipy.apigateway.APIController.ProductBookingStatus
import ru.quipy.apigateway.APIController.ProductBookingStatus.*
import ru.quipy.core.EventSourcingService
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import ru.quipy.warehouse.api.BookingAggregate
import ru.quipy.warehouse.api.BookingCreatedEvent
import ru.quipy.warehouse.api.ProductAggregate
import ru.quipy.warehouse.api.ProductCreatedEvent
import ru.quipy.warehouse.logic.*
import java.util.*
import javax.annotation.PostConstruct

@Service
class WarehouseBalanceService {

    val logger: Logger = LoggerFactory.getLogger(WarehouseBalanceService::class.java)

    @Autowired
    private lateinit var bookingESService: EventSourcingService<UUID, BookingAggregate, BookingAggregateState>

    @Autowired
    private lateinit var productBalanceRepository: ProductBalanceRepository

    @Autowired
    private lateinit var bookingLogRepository: BookingLogRepository

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(
            ProductAggregate::class,
            "wh-balance-products-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(ProductCreatedEvent::class) { event ->
                val product = productBalanceRepository.findById(event.productID)
                if (product.isPresent) return@`when`

                productBalanceRepository.save(ProductBalance(event.productID, event.title, event.price, 10_000_000))
                logger.info("Wh balance increase for product: ${event.title}, amount: 10_000")
            }
        }

        subscriptionsManager.createSubscriber(
            BookingAggregate::class,
            "wh-balance-bookings-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(BookingCreatedEvent::class) { event ->
                appExecutor.submit {
                    logger.info("Booking created: ${event.bookingId} for order ${event.orderId} products: ${event.products}")

                    event.products.map {
                        val dbProduct = productBalanceRepository.findById(it.productId).get()
                            ?: throw RuntimeException("No such product ${it.productId}")
                        BookedProduct(dbProduct.id, it.amount, dbProduct.price)
                    }.let { booked ->
                        bookingESService.update(event.bookingId) {
                            it.succeeded(booked)
                        }

                        bookingLogRepository.saveAll(
                            booked.map {
                                BookingLogRecord(
                                    UUID.randomUUID().toString(),
                                    event.bookingId.toString(),
                                    it.productId.toString(),
                                    SUCCESS,
                                    it.amount,
                                    System.currentTimeMillis()
                                )
                            }
                        )

                        logger.info(
                            "Booking succeeded. ${event.bookingId}. Products: ${
                                booked.map { it.productId }.toList()
                            }"
                        )
                    }
                }

                // we don't need real-life booking strategy for the fault-tolerance course
//                coroutineMutex.withLock {
//                    logger.info("Booking created: ${event.bookingId} for order ${event.orderId} products: ${event.products}")
//                    val succeeded = mutableListOf<BookedProduct>()
//                    val failed = mutableListOf<FailedProduct>()
//                    event.products.forEach { product ->
//                        val dbProduct = productBalanceRepository.findById(product.productId)
//
//                        if (dbProduct.isPresent) {
//                            val balance = dbProduct.get()
//                            if (balance.amount < product.amount) {
//                                failed.add(FailedProduct(product.productId, product.amount, "Not enough products"))
//                            } else {
//                                balance.amount -= product.amount
//                                productBalanceRepository.save(balance)
//                                logger.info("For product ${balance.id} booked ${product.amount} items")
//                                succeeded.add(BookedProduct(product.productId, dbProduct.get().price, product.amount))
//                            }
//                        } else {
//                            failed.add(FailedProduct(product.productId, product.amount, "No such product"))
//                        }
//                    }
//                    if (failed.isEmpty()) {
//                        succeeded.forEach {
//                            bookingLogRepository.save(
//                                BookingLogRecord(
//                                    UUID.randomUUID(),
//                                    event.bookingId,
//                                    it.productId,
//                                    SUCCESS,
//                                    it.amount,
//                                    System.currentTimeMillis()
//                                )
//                            )
//                        }
//
//                        bookingESService.update(event.bookingId) {
//                            it.succeeded(succeeded)
//                        }
//
//                        logger.info("Booking ${event.bookingId} succeeded")
//                    } else {
//                        succeeded.forEach { product ->
//                            productBalanceRepository.findById(product.productId).map {
//                                it.amount += product.amount
//                                productBalanceRepository.save(it)
//                                logger.info("For product ${product.productId} unbooked ${product.amount} items")
//                            }
//                            bookingLogRepository.save(
//                                BookingLogRecord(
//                                    UUID.randomUUID(),
//                                    event.bookingId,
//                                    product.productId,
//                                    SUCCESS,
//                                    product.amount,
//                                    System.currentTimeMillis()
//                                )
//                            )
//                        }
//
//                        failed.forEach { product ->
//                            bookingLogRepository.save(
//                                BookingLogRecord(
//                                    UUID.randomUUID(),
//                                    event.bookingId,
//                                    product.productId,
//                                    FAILED,
//                                    product.amount,
//                                    System.currentTimeMillis()
//                                )
//                            )
//                        }
//
//                        bookingESService.update(event.bookingId) {
//                            it.failed(failed)
//                        }
//                        logger.info("Booking ${event.bookingId} failed")
//                    }
//                }
            }
        }
    }

    @Document
    data class ProductBalance(
        @Id
        val id: UUID,
        val title: String,
        val price: Int,
        var amount: Int,
    )

    @Document
    class BookingLogRecord(
        @Id
        val id: String,
        @Indexed
        val bookingId: String,
        val itemId: String,
        val status: ProductBookingStatus,
        val amount: Int,
        val timestamp: Long = System.currentTimeMillis(),
    )
}

@Repository
interface ProductBalanceRepository : MongoRepository<WarehouseBalanceService.ProductBalance, UUID>

@Repository
interface BookingLogRepository : MongoRepository<WarehouseBalanceService.BookingLogRecord, UUID> {
    fun findAllByBookingId(bookingId: String): List<WarehouseBalanceService.BookingLogRecord>
}
