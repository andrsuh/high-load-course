package ru.quipy.apigateway

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*
import java.util.concurrent.RejectedExecutionException

@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @Autowired
    private lateinit var ingressRateLimiter: IngressRateLimiter

    @Value($$"${ingress.retry-after-ms}")
    private var retryAfterMs: Long = 0L

    @PostConstruct
    fun logRetryAfterConfiguration() {
        logger.warn(
            "Ingress 429 responses use Retry-After: {} second(s) (configured {} ms)",
            retryAfterMs / 1000L,
            retryAfterMs
        )
    }

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
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<PaymentSubmissionDto> {
        val remainingMs = deadline - System.currentTimeMillis()
        if (remainingMs < MIN_REMAINING_BUDGET_MS) {
            logger.warn("Payment for order $orderId rejected by deadline guard: remaining={} ms", remainingMs)
            return tooManyRequests()
        }

        if (!ingressRateLimiter.tryAcquire(INGRESS_WAIT_MAX_MS)) {
            logger.warn("Payment for order $orderId rejected by ingress rate limiter")
            return tooManyRequests()
        }

        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )

    companion object {
        private const val MIN_REMAINING_BUDGET_MS = 300L
        private const val INGRESS_WAIT_MAX_MS = 300L
    }

    @ExceptionHandler(RejectedExecutionException::class)
    fun onPaymentQueueFull(): ResponseEntity<Unit> =
        ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .header(HttpHeaders.RETRY_AFTER, (retryAfterMs / 1000L).toString())
            .build()

    private fun tooManyRequests(): ResponseEntity<PaymentSubmissionDto> =
        ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .header(HttpHeaders.RETRY_AFTER, (retryAfterMs / 1000L).toString())
            .build()
}