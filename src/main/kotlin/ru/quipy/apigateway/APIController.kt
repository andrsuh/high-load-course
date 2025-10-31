package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.*
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl
import java.time.Duration.ofMillis
import java.time.Duration.ofSeconds
import java.util.*
import java.util.concurrent.ConcurrentHashMap

@RestController
class APIController(
    adapters: List<PaymentExternalSystemAdapter>
) {
    private val defaultAdapter = adapters.first() as? PaymentExternalSystemAdapterImpl
    private val properties = defaultAdapter!!.properties

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    private val retryCounters = ConcurrentHashMap<UUID, Pair<Int, Long>>()
    private val maxRetries = 2
    private val retryWindowMs = 60_000L


    private val slidingWindow = SlidingWindowRateLimiter(
        rate = 10,
        window = ofSeconds(1)
    )
    
    private val leakingBucket = LeakingBucketRateLimiter(
        rate = 10,
        bucketSize = 38,
        window = ofMillis(1000)
    )
    
    private val compositeRateLimiter = CompositeRateLimiter(
        rl1 = slidingWindow,
        rl2 = leakingBucket,
        mode = CompositeMode.AND
    )

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
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<*> {
        val now = System.currentTimeMillis()

        val (count, lastTime) = retryCounters[orderId] ?: (0 to now)
        if (now - lastTime > retryWindowMs) {
            retryCounters[orderId] = (1 to now)
        } else if (count >= maxRetries) {
            logger.debug("Too many retries for order {}", orderId)
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(mapOf("error" to "Too many retries for order $orderId"))
        } else {
            retryCounters[orderId] = (count + 1 to now)
        }

        val remaining = deadline - now
        val avgProcessingMs = properties.averageProcessingTime.toMillis()
        if (remaining < avgProcessingMs) {
            logger.debug("Deadline too close, skipping payment for order {} (remaining={} ms)", orderId, remaining)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(mapOf("error" to "Deadline too close — payment no longer meaningful"))
        }

        if (!compositeRateLimiter.tick()) {
            logger.debug("Rate limit exceeded for payment request of {}", orderId)

            val retryAfterMs = 500L
            return if (remaining > avgProcessingMs + retryAfterMs) {
                ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("X-Retry-After-Ms", retryAfterMs.toString())
                    .body(mapOf("error" to "Rate limit exceeded, retry after $retryAfterMs ms"))
            } else {
                ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(mapOf("error" to "Rate limit exceeded and deadline too close"))
            }
        }

        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(mapOf("error" to "No such order $orderId"))

        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }


    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
