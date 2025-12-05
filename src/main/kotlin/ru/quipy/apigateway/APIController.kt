package ru.quipy.apigateway

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.exceptions.TooManyRequestsException
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
        rate = 10,
        window = Duration.ofMillis(1200))

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    @Autowired
    lateinit var meterRegistry: MeterRegistry

    private val retryCounter by lazy {
        meterRegistry.counter(
            "payment_retry",
            "service", "cas-m3404-07"
        )
    }

    private val simpleCounter by lazy {
        meterRegistry.counter(
            "pay_order_requests",
            "service", "cas-m3404-07"
        )
    }

    private val sendRetryCounter by lazy {
        meterRegistry.counter(
            "create_order_429_sent",
            "service", "cas-m3404-07"
        )
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
    fun payOrder(
        @PathVariable orderId: UUID,
        @RequestParam deadline: Long
    ): ResponseEntity<Any> {

        val paymentId = UUID.randomUUID()

        val order = orderRepository.findById(orderId)
            ?: throw IllegalArgumentException("No such order $orderId")

        val isRetry = order.status != OrderStatus.COLLECTING
        if (isRetry) {
            retryCounter.increment()
        }

        simpleCounter.increment()

        val now = Instant.now().toEpochMilli()
        val averageProcessingTime = 1200

        if (!limiter.tick()) {
            if (deadline < now + averageProcessingTime) {
                return ResponseEntity
                    .status(HttpStatus.GONE)
                    .body(mapOf("error" to "Deadline will expire before processing"))
            }

            sendRetryCounter.increment()

            throw TooManyRequestsException(
                20,
                "Rate limit exceeded. Try again later."
            )
        }

        orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))

        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)

        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }


    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}