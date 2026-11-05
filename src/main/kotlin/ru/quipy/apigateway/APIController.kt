package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit

@RestController
class APIController(
    private val orderRepository: OrderRepository,
    private val orderPayer: OrderPayer,
    @Value("\${payment.admission.rate-per-second}") admissionRate: Int,
    @Value("\${payment.admission.capacity}") admissionCapacity: Int,
    @param:Value("\${payment.admission.retry-after-seconds}") private val retryAfterSeconds: Int,
) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    private val paymentAdmission = TokenBucketRateLimiter(admissionRate, admissionCapacity, 1, TimeUnit.SECONDS)

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
        val order = orderRepository.findById(orderId) ?: throw IllegalArgumentException("No such order $orderId")
        if (!paymentAdmission.tick()) throw RejectedExecutionException("Payment admission limit reached")

        val paymentId = UUID.randomUUID()
        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
        return PaymentSubmissionDto(createdAt, paymentId)
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )

    @ExceptionHandler(RejectedExecutionException::class)
    fun paymentRejected(): ResponseEntity<Map<String, String>> =
        ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .header(HttpHeaders.RETRY_AFTER, retryAfterSeconds.toString())
            .body(mapOf("message" to "Payment capacity reached. Retry later."))
}
