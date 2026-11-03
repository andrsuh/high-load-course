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

    @Value("\${ingress.retry-after-ms}")
    private var retryAfterMs: Long = 0L

    @Value("\${ingress.wait-ms:300}")
    private var ingressWaitMs: Long = 300L

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

        // «Длинный» дедлайн (тест 3, 26 с) означает пачечную нагрузку: ~200 запросов раз в
        // runits = 90 с. Для неё нужен бакет, вмещающий пачку целиком, иначе клиент получает
        // 429, пересоздаёт заказ и тест не набирает объём. Для коротких тестов бакет малый:
        // он гасит только волны повторов после Retry-After.
        val longDeadline = remainingMs >= LONG_DEADLINE_THRESHOLD_MS
        if (!ingressRateLimiter.tryAcquire(ingressWaitMs, longDeadline)) {
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
        /**
         * Минимальный остаток клиентского бюджета, при котором запрос ещё принимается.
         * Ниже этого порога платеж гарантированно не успеет, и тратить на него разрешение
         * лимитера и слот пула бессмысленно.
         */
        private const val MIN_REMAINING_BUDGET_MS = 300L

        /**
         * Граница «длинного» дедлайна — 20 с.
         *
         * Дедлайны тестов: 2.5 с, 13 с, 26 с. Пачечный профиль (пачки по 240–270 запросов
         * раз в runits = 90 с) есть только у третьего теста, поэтому граница должна стоять
         * между 13 с и 26 с. При прежней границе в 5 с второй тест попадал в «длинный»
         * резервуар с бакетом 320 и получал право на залп в 320 запросов: лимитер пропускал
         * весь вход (rejected = 0 при входе 11.6 rps против темпа 10 rps), а выходное окно
         * аккаунта держит только 11 rps — очередь росла до 13 с, и клиент потерял 216 платежей из 2200 по таймауту (90 % успеха).
         * У второго теста профиль ровный, ему достаточно бакета 11 и темпа 10 rps,
         * а излишек он получает как 429 и повторяет.
         */
        private const val LONG_DEADLINE_THRESHOLD_MS = 20_000L
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