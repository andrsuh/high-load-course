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
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.RateLimiter
import java.time.Duration
import ru.quipy.payments.logic.PaymentAccountProperties
import java.util.concurrent.TimeUnit
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min 
import kotlin.math.ceil

@RestController
class APIController(
    private val metrics: HttpMetrics
) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @Volatile
    private var rateLimiter: TokenBucketRateLimiter? = null
    private val limiterLock = Any()

    private var tooManyReqDeadline: Long = 1000L
    private var minProcessingTime: Long = 0
    private var processingSpeed: Double = 0.0
    @Volatile
    private var clientCanWait: Long? =  null
    private var canRestInQueue: Long = 26000

    private fun processingSpeed(property : PaymentAccountProperties) : Double{
        return kotlin.math.min(property.rateLimitPerSec.toDouble(), property.parallelRequests.toDouble() / property.averageProcessingTime.toSeconds())
    }

    private fun createLimiter(): TokenBucketRateLimiter {
        processingSpeed = orderPayer.getAccountsProperties().minOf { p -> processingSpeed(p)}
        minProcessingTime = orderPayer.getAccountsProperties().minOf { p -> p.averageProcessingTime}.toMillis()

        canRestInQueue = clientCanWait!! - minProcessingTime - 500
        canRestInQueue = max(canRestInQueue,0) 

        var supportSizeQueue =((canRestInQueue / 1000.0) * processingSpeed).toInt()

        logger.info(
            "Creating TokenBucketRateLimiter (clientCanWait=$clientCanWait) " +
                    "rate={}, bucketSize={}, process all queue={} retry-ater {}",
            processingSpeed.toLong(),
            max(processingSpeed.toInt(), supportSizeQueue),
            tooManyReqDeadline+ canRestInQueue,
            tooManyReqDeadline
        )

        return TokenBucketRateLimiter(
            rate = processingSpeed.toInt(),
            bucketMaxCapacity =  max(processingSpeed.toInt(), supportSizeQueue),
            window = 1,
            timeUnit = TimeUnit.SECONDS
        )

        // return  LeakingBucketRateLimiter (
        //     rate = processingSpeed.toLong(),
        //     window = Duration.ofSeconds(1),
        //     bucketSize = max(processingSpeed.toInt(), supportSizeQueue)
        // )
    }

    private fun getOrCreateLimiter(canWait: Long): TokenBucketRateLimiter {

        if (clientCanWait != null && rateLimiter != null) {
            val relativeError = abs(canWait - clientCanWait!!) / abs(clientCanWait!!)
            if (relativeError <= 0.01){
                return rateLimiter!!
            }
        }

        synchronized(limiterLock) {
            if (clientCanWait != null && rateLimiter != null) {
                val relativeError = abs(canWait - clientCanWait!!) / abs(clientCanWait!!)
                if (relativeError <= 0.01){
                    return rateLimiter!!
                }
            }

            clientCanWait = canWait
            rateLimiter = createLimiter()
            return rateLimiter!!
        }
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
        val limiter = getOrCreateLimiter(deadline - System.currentTimeMillis())
        if (!limiter.tick()){
            metrics.toManyRespCounter.increment()
            val numberOfRequests = orderPayer.getNumberOfRequests()+limiter.size()
            metrics.inQueueCount.set(numberOfRequests)
            tooManyReqDeadline = ((numberOfRequests.toDouble()  / processingSpeed) * 1000 * ((minProcessingTime)/1000.0) ).toLong()
            tooManyReqDeadline -= canRestInQueue

            val dead =  System.currentTimeMillis() + tooManyReqDeadline
            metrics.toManyRequestsDelayTime.record(dead-System.currentTimeMillis(), TimeUnit.MILLISECONDS)

            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).header("Retry-After", dead.toString()).build();
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