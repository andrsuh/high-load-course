package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.common.utils.RateLimiter
import java.util.*

@RestController
class APIController(
    private val orderRateLimiter: RateLimiter,
    private val paymentRateLimiter: RateLimiter,
    private val incomingPaymentRateLimiter: RateLimiter
) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

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
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<Any> {
        val now = System.currentTimeMillis()

        if (!orderPayer.canAcceptRequest()) {
            val queueSize = orderPayer.getQueueSize()
            val retryAfterMs = minOf(500, 50 + queueSize * 5)

            val response = ErrorResponse(
                message = "System overloaded. Please retry later.",
                retryAfter = retryAfterMs
            )

            return ResponseEntity
                .status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", (retryAfterMs / 1000).toString()) // секунды по стандарту
                .body(response)
        }

        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: return ResponseEntity
            .status(HttpStatus.NOT_FOUND)
            .body(ErrorResponse("No such order $orderId", 0))

        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        val response = PaymentSubmissionDto(createdAt, paymentId)

        return ResponseEntity.ok(response)
    }

    data class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )

    data class ErrorResponse(
        val message: String,
        val retryAfter: Int
    )
}
