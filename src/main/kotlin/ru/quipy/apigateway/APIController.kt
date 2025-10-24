package ru.quipy.apigateway

import jakarta.annotation.PostConstruct          
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.common.utils.RateLimiter
import java.time.Duration
import ru.quipy.payments.logic.PaymentAccountProperties
import java.util.concurrent.TimeUnit

@RestController
class APIController(
    private val metrics: HttpMetrics
) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    private lateinit var rateLimiter: TokenBucketRateLimiter

    private var tooManyReqDeadline: Long = 1000L
    private var minProcessingTime: Long = 0
    private var processingSpeed: Double = 0.0
    private var clientCanWait: Long = 13000
    private var canRestInQueue: Long = 26000

    private fun processingSpeed(property : PaymentAccountProperties) : Double{
        return kotlin.math.min(property.rateLimitPerSec.toDouble(), property.parallelRequests.toDouble() / property.averageProcessingTime.toSeconds())
    }

    @PostConstruct
    fun initRateLimiter() {
        processingSpeed = orderPayer.getAccountsProperties().minOf { p -> processingSpeed(p)}
        minProcessingTime = orderPayer.getAccountsProperties().minOf { p -> p.averageProcessingTime}.toMillis()

        canRestInQueue =  clientCanWait - 2000
        // val supportSizeQueue = ((canRestInQueue/1000.0) * processingSpeed / (minProcessingTime/1000.0+2)).toInt()
        val supportSizeQueue = ((canRestInQueue/1000.0) * processingSpeed).toInt()
        tooManyReqDeadline = ((supportSizeQueue.toDouble() / processingSpeed) * (minProcessingTime/1000+2) * 1000).toLong()
        tooManyReqDeadline = tooManyReqDeadline -  canRestInQueue


        logger.info("Creating rate limit with rate: {} window: {} bucketSize: {} process all queue {}", processingSpeed.toLong(),Duration.ofSeconds(1),supportSizeQueue,tooManyReqDeadline+canRestInQueue)

        // rateLimiter =  LeakingBucketRateLimiter (
        //     rate = processingSpeed.toLong(),
        //     window = Duration.ofSeconds(1),
        //     bucketSize = supportSizeQueue
        // )

        rateLimiter = TokenBucketRateLimiter(
                rate = processingSpeed.toInt(),
                bucketMaxCapacity = supportSizeQueue,
                window = 1,
                timeUnit = TimeUnit.SECONDS
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
        logger.info("Creating order with ID: {}", order.id)
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
        metrics.requestsCounter.increment()
        if (!rateLimiter.tick()){
            metrics.toManyRespCounter.increment()
            // val queueSize = rateLimiter.size()
            // tooManyReqDeadline = ((queueSize.toDouble() / processingSpeed) * (minProcessingTime/1000+1) * 1000).toLong()
            // tooManyReqDeadline = tooManyReqDeadline -  canRestInQueue
            // val dead =  System.currentTimeMillis() + tooManyReqDeadline
            // metrics.toManyRequestsDelayTime.record(tooManyReqDeadline, TimeUnit.MILLISECONDS)
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build();
            // return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).header("Retry-After", dead.toString()).build();
        }
        metrics.requestsCounter2.increment()
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        val (createdAt, success, deadline) = orderPayer.processPayment(orderId, order.price, paymentId, deadline,metrics)

        if (!success){
            metrics.toManyRespCounter2.increment()
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).header("Retry-After", deadline.toString()).build();
        }
        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}