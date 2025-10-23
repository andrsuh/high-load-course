package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
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
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): PaymentSubmissionDto {
        val now = System.currentTimeMillis()

        if (!incomingPaymentRateLimiter.tick()) {
            // RFC 7231: Retry-After in SECONDS (delay-seconds format)
            // Token available every ~91ms (1000ms / 11 RPS)
            // Use 0 seconds = immediate retry for maximum throughput
            throw TooManyRequestsException("Rate limit exceeded. Retry-After: 0")
        }

        if (!orderPayer.canAcceptRequest()) {
            // RFC 7231: Retry-After in SECONDS (delay-seconds format)
            // Use 0 seconds = immediate retry
            throw TooManyRequestsException("System overloaded. Current queue: ${orderPayer.getQueueSize()}. Retry-After: 0")
        }

        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        return PaymentSubmissionDto(createdAt, paymentId)
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )

    @ExceptionHandler(TooManyRequestsException::class)
    fun handleTooManyRequests(ex: TooManyRequestsException): ResponseEntity<ErrorResponse> {

        val retryAfterSeconds = ex.message?.substringAfter("Retry-After: ")?.toIntOrNull() ?: 0

        return ResponseEntity
            .status(HttpStatus.TOO_MANY_REQUESTS)
            .header("Retry-After", retryAfterSeconds.toString())
            .body(ErrorResponse(ex.message ?: "Too many requests", retryAfterSeconds))
    }

    data class ErrorResponse(val message: String, val retryAfter: Int)
}

class TooManyRequestsException(message: String) : RuntimeException(message)