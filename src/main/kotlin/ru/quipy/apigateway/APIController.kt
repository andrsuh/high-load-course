package ru.quipy.apigateway

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.time.Duration
import java.time.Instant
import java.util.*


@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)


    @Autowired
    private lateinit var orderRepository: OrderRepository

    private val limiter = SlidingWindowRateLimiter(
        rate = 8,
        window = Duration.ofMillis(1200))

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    @Autowired
    private lateinit var prometheusRegistry: PrometheusMeterRegistry

    private val retryCounter = prometheusRegistry.counter(
        "payment.retry.total",
        "service", "cas-m3404-07"
    )




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

        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        val isRetry = order.status != OrderStatus.COLLECTING

        if (isRetry) {
            retryCounter.increment()
        }

        val now = Instant.now().toEpochMilli()
        val averageProcessingTime = 1200

        if (!limiter.tick()) {
            if (deadline < now + averageProcessingTime) {
                return ResponseEntity
                    .status(HttpStatus.GONE)
                    .body(mapOf("error" to "Deadline will expire before processing"))
            }

            return ResponseEntity
                .status(HttpStatus.TOO_MANY_REQUESTS)
                .body(mapOf("error" to "Rate limit exceeded. Try again later."))
        }


        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)

        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }


    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}