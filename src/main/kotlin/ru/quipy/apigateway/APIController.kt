package ru.quipy.apigateway

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.TokenBucketRateLimiter

import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*
import java.util.concurrent.TimeUnit

@RestController
class APIController(
    @Autowired
    var registry: MeterRegistry
) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    //Для третьего теста  private var rateLimiter = TokenBucketRateLimiter(11, 284, 1, TimeUnit.SECONDS)
    /*Мы обратили внимание на график Amount of queries, и также прочитали конфигурации аккаунта, максимальная пропускная способность это 11 rps (rateLimitPerSec=11),
     поэтому мы ограничили rate до 11.
     Так как у нас bucketMaxCapacity = rate, то поведение становится строго равномерным , то есть лимитер выдаёт запросы максимально стабильно, без резких всплесков. */
    private var rateLimiter = TokenBucketRateLimiter(11, 11, 1, TimeUnit.SECONDS)
    /*Для третьего кейса processingTimeMillis = 26000, bucketMaxCapacity = 11 req/s * 26 s = 286 допустимых запросов - взяли чуть поменьше 284.*/
    private val counter = Counter.builder("queries.amount").tag("name", "orders").register(registry)
    private val counterPayment = Counter.builder("queries.amount").tag("name", "payment").register(registry)

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        counter.increment()
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        var save = orderRepository.save(order)
        return save
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
        val paymentId = UUID.randomUUID()

        // Для третьего теста меняем на 700
        /* Используем timestamp, чтобы определить, через сколько миллисекунд нужно повторить запрос.
Если поставить слишком большое значение: клиент ждёт дольше, чем реально нужно, не укладываемся по времени в 6 минут, поэтому сокращаем до 700 */
        val timestamp = System.currentTimeMillis() + 950
        /*По тесту токены добавляются каждую секунду (1000 мс). Установка Retry-After = 950 мс позволяет клиентам начать повторные попытки чуть раньше, чем появится новый токен. Сделано для снижения риска накопления очереди запросов.*/
        if (!rateLimiter.tick()) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", timestamp.toString())
                .build()
        }


        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        counterPayment.increment()
        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)

        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}