package ru.quipy.apigateway

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.metrics.MetricsCollector
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.time.Duration
import java.util.*

@RestController
class APIController(
    private val metricsCollector: MetricsCollector
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
    private lateinit var rateLimiter : LeakingBucketRateLimiter
    @PostConstruct
    fun init() {
        val limit = orderPayer.getMaxRateLimit()
        this.rateLimiter = LeakingBucketRateLimiter(limit.toLong(), Duration.ofSeconds(1), 400)
    }


    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<PaymentSubmissionDto> {
        if (!rateLimiter.tick()) {
            metricsCollector.status429RequestInc()
            return ResponseEntity
                .status(HttpStatus.TOO_MANY_REQUESTS)
                .build()
        }

        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")


        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        if (createdAt == null){
            metricsCollector.status429RequestInc()
            return ResponseEntity
                .status(HttpStatus.TOO_MANY_REQUESTS)
                .build()
        }

        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}