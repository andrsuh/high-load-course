package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.core.EventSourcingService
import ru.quipy.delivery.api.DeliveryAggregate
import ru.quipy.delivery.logic.DeliveryAggregateState
import ru.quipy.delivery.logic.create
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.logic.*
import ru.quipy.orders.projection.OrderCache
import ru.quipy.orders.projection.OrderRepository
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.projections.FinancialLogRepository
import ru.quipy.payments.subscribers.OrderPayer
import ru.quipy.payments.subscribers.PaymentTransactionsSubscriber
import ru.quipy.payments.subscribers.PaymentTransactionsSubscriber.PaymentLogRecord
import ru.quipy.warehouse.api.BookingAggregate
import ru.quipy.warehouse.api.ProductAggregate
import ru.quipy.warehouse.logic.BookingAggregateState
import ru.quipy.warehouse.logic.ProductAggregateState
import ru.quipy.warehouse.projections.BookingLogRepository
import ru.quipy.warehouse.projections.ProductBalanceRepository
import java.time.Duration
import java.util.*

@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

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

    @Autowired
    private lateinit var productBalanceRepository: ProductBalanceRepository

    @Autowired
    private lateinit var financialLogRepository: FinancialLogRepository

    @Autowired
    private lateinit var bookingLogRepository: BookingLogRepository

    @Autowired
    private lateinit var orderCache: OrderCache

    @Autowired
    private lateinit var paymentsLog: PaymentTransactionsSubscriber

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            emptyMap(),
            null,
            null,
            price,
            emptyList()
        )
        return orderRepository.save(order)
    }

    @GetMapping("/orders/{orderId}")
    suspend fun getOrder(@PathVariable orderId: UUID): Order {
        return orderRepository.findById(orderId) ?: throw IllegalArgumentException("No such order $orderId")
    }
    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val itemsMap: Map<UUID, Int>,
        val deliveryId: UUID? = null,
        val deliveryDuration: Long? = null,
        val price: Int,
        val paymentHistory: List<PaymentLogRecord> = mutableListOf()
    )

    data class OrderItem(
        val id: UUID,
        val title: String,
        val price: Int = 100
    )

    @GetMapping("/items")
    fun getItems(@RequestParam available: Boolean = true): List<CatalogItem> {
        return productBalanceRepository.findAll().filter { it.amount > 0 }.map {
            CatalogItem(it.id, it.title, "", price = 100, it.amount)
        }
    }

    data class CatalogItem(
        val id: UUID,
        val title: String,
        val description: String,
        val price: Int = 100,
        val amount: Int, // number of items allowed for booking
    )

    @PutMapping("/orders/{orderId}/items/{itemId}")
    fun putItemToOrder(@PathVariable orderId: UUID, @PathVariable itemId: UUID, @RequestParam amount: Int): Boolean {
        val events = ordersESService.update(orderId) {
            it.addItem(itemId, amount)
        }.also {
            orderCache.invalidateOrderState(orderId)
        }
        return events.isNotEmpty()
    }

    @PostMapping("/orders/{orderId}/bookings")
    suspend fun startCheckout(@PathVariable orderId: UUID): Any {
        try {
            logger.info("Checkout initiated for order $orderId")
            val event = ordersESService.update(orderId) {
                it.checkout()
            }
            logger.info("Checkout started ${event.bookingId} for order $orderId")
            return BookingDto(
                event.bookingId,
                emptySet()
            )
        } catch (e: Exception) {
            logger.error("Booking failed for order $orderId", e)
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Booking timed out")
        }
    }

    data class BookingDto(
        val id: UUID,
        val failedItems: Set<UUID> = emptySet()
    )

    // mocked!! available delivery durations in seconds. Like: "Order would be delivered in N milliseconds"
    private val slots = listOf(3L, 5L, 8L, 10L)
    @GetMapping("/orders/delivery/slots")
    fun getDeliverySlots(): List<Long> {
        return slots
    }


    @PostMapping("/orders/{orderId}/delivery")
    fun setDeliverySlots(@PathVariable orderId: UUID, @RequestParam slot: Long): UUID {
        return deliveryESService.create {
            it.create(UUID.randomUUID(), orderId, Duration.ofSeconds(slot).toMillis())
        }.deliveryId.also {
            orderCache.invalidateOrderState(orderId)
        }
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID): PaymentSubmissionDto {
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")


        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId)
        return  PaymentSubmissionDto(createdAt, paymentId)
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )

    @GetMapping("/orders/{orderId}/finlog")
    fun getUserFinancialHistory(@PathVariable orderId: String): List<UserAccountFinancialLogRecord> {
        return financialLogRepository.findAllByOrderId(orderId).map {
            UserAccountFinancialLogRecord(
                it.type,
                it.amount,
                UUID.fromString(it.orderId),
                it.paymentId,
                it.timestamp
            )
        }
    }

    data class UserAccountFinancialLogRecord(
        val type: FinancialOperationType,
        val amount: Int,
        val orderId: UUID,
        val paymentId: UUID,
        val timestamp: Long
    )

    enum class FinancialOperationType {
        REFUND,
        WITHDRAW
    }

    @GetMapping("/_internal/bookingHistory/{bookingId}")
    fun getBookingHistory(@PathVariable bookingId: String): List<BookingLogRecord> {
        return bookingLogRepository.findAllByBookingId(bookingId).map {
            BookingLogRecord(
                UUID.fromString(it.bookingId),
                UUID.fromString(it.itemId),
                it.status,
                it.amount,
                it.timestamp,
            )
        }
    }

    class BookingLogRecord(
        val bookingId: UUID,
        val itemId: UUID,
        val status: ProductBookingStatus,
        val amount: Int,
        val timestamp: Long = System.currentTimeMillis(),
    )

    enum class ProductBookingStatus {
        FAILED,
        SUCCESS
    }
}