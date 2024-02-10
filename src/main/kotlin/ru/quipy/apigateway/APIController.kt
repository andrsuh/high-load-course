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
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.projections.FinancialLogRepository
import ru.quipy.warehouse.api.BookingAggregate
import ru.quipy.warehouse.api.ProductAggregate
import ru.quipy.warehouse.logic.BookingAggregateState
import ru.quipy.warehouse.logic.BookingState
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

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID): Order {
        val event = ordersESService.create { it.create(UUID.randomUUID(), userId) }
        return Order(
            event.orderId,
            event.userId,
            event.createdAt,
            OrderStatus.COLLECTING,
            emptyMap(),
            null,
            null,
            emptyList()
        )
    }

    @GetMapping("/orders/{orderId}")
    suspend fun getOrder(@PathVariable orderId: UUID): Order {
        val state = orderCache.getOrderState(orderId) ?: throw IllegalArgumentException("No such order $orderId")
        return Order(
            state.getId(),
            state.userId,
            state.createdAt,
            state.status,
            state.shoppingCart.items.associate { it.productId to it.amount },
            state.deliveryId,
            state.deliveryId?.let { getDeliveryDuration(it) },
            state.paymentId?.let { getPaymentInfo(it) } ?: emptyList()
        )
    }

    private fun getDeliveryDuration(deliveryId: UUID): Long? {
        val deliveryAggregateState =
            deliveryESService.getState(deliveryId) ?: throw IllegalArgumentException("No such delivery $deliveryId")
        return deliveryAggregateState.deliveryDuration
    }

    private fun getPaymentInfo(paymentId: UUID): List<PaymentLogRecord> {
        val payment = paymentESService.getState(paymentId) ?: return emptyList()

        if (payment.processings.isEmpty()) return emptyList()

        return payment.processings.values.map {
            PaymentLogRecord(
                it.processedAt,
                status = if (it.success) PaymentStatus.SUCCESS else PaymentStatus.FAILED,
                payment.amount!!,
                paymentId
            )
        }
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val itemsMap: Map<UUID, Int>,
        val deliveryId: UUID? = null,
        val deliveryDuration: Long? = null,
        val paymentHistory: List<PaymentLogRecord>
    )

    class PaymentLogRecord(
        val timestamp: Long,
        val status: PaymentStatus,
        val amount: Int,
        val transactionId: UUID,
    )

    enum class PaymentStatus {
        FAILED,
        SUCCESS
    }

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
        val paymentCreated = ordersESService.update(orderId) {
            it.startPayment()
        }.also {
            orderCache.invalidateOrderState(orderId)
        }
        return PaymentSubmissionDto(paymentCreated.createdAt, paymentCreated.paymentId)
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