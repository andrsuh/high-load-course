package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.ratelimiter.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.pow
import kotlin.random.Random


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val scheduledExecutorScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val maxQueueSize = 5000
    private val queue = PriorityBlockingQueue<PaymentRequest>(maxQueueSize)

    private val outgoingRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L))
    private val inFlightRequests = AtomicInteger(0)

    val queuedRequestsMetric = Gauge.builder("payment_queued_requests", queue) { queue.size.toDouble() }
        .description("Total number of queued requests")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    private val backPressureGauge = Gauge.builder("payment_backpressure_ratio") {
        (queue.size.toDouble() / maxQueueSize.toDouble()).coerceIn(0.0, 1.0)
    }
        .description("Current back pressure level [0..1]")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    val actualTimeout = Counter.builder("payment_actual_timeout_count")
        .description("Count total amount of actual timeouts")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    val theoreticalTimeout = Counter.builder("payment_theoretical_timeout_count")
        .description("Count total amount of theoretical timeouts")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    override fun canAcceptPayment(deadline: Long): Pair<Boolean, Long> {
        val estimatedWaitMs = ((queue.size / rateLimitPerSec.toDouble()) + 1) * 1000
        val willCompleteAt = now() + estimatedWaitMs + requestAverageProcessingTime.toMillis()

        val canMeetDeadline = willCompleteAt < deadline
        val queueOk = queue.size < maxQueueSize

        return Pair(canMeetDeadline && queueOk, willCompleteAt.toLong())
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val (canAccept, expectedCompletionMillis) = canAcceptPayment(deadline)
        if (!canAccept) {
            logger.error("429 from PaymentExternalSystemAdapterImpl")
            val delaySeconds = (expectedCompletionMillis - System.currentTimeMillis()) / 1000
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                delaySeconds.toString(),
            )
        }

        val paymentRequest = PaymentRequest(deadline) {
            executePayment(paymentId, amount, paymentStartedAt, deadline)
        }

        val accepted = queue.offer(paymentRequest)
        if (!accepted) {
            logger.error("429 from PaymentExternalSystemAdapterImpl (queue reason)")
            logger.error("[$accountName] Queue overflow! Rejecting payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Queue overflow (back pressure).")
            }
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                "All payment accounts are under back pressure. Try again later."
            ).also {
                it.headers.add("Retry-After", "5")
            }
        }
    }


    fun executePayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        val retryManager = RetryManager(
            maxRetries = 3,
            baseDelayMillis = 100,
            backoffFactor = 2.0,
            jitterMillis = 50
        )

        var lastError: Exception? = null

        while (retryManager.shouldRetry(now(), deadline)) {
            try {
                client.newCall(request).execute().use { response ->
                    val respBodyStr = response.body?.string() ?: ""
                    val body = try {
                        mapper.readValue(respBodyStr, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Bad response for txId: $transactionId, payment: $paymentId, code: ${response.code}, body: $respBodyStr", e)
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, code: ${response.code}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    if (body.result) {
                        return
                    } else {
                        lastError = Exception(body.message)
                        retryManager.onFailure()
                    }
                }
            } catch (e: SocketTimeoutException) {
                logger.error("[$accountName] Timeout for txId: $transactionId, payment: $paymentId", e)
                lastError = e
                retryManager.onFailure()
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                lastError = e
                retryManager.onFailure()
            }
        }

        val reason = when {
            now() >= deadline -> "Deadline exceeded."
            lastError != null -> lastError.message ?: "Unknown error"
            else -> "Payment failed after retries."
        }

        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }

        logger.error("[$accountName] Payment failed after retries for txId: $transactionId, payment: $paymentId — reason: $reason")
    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun pollQueue() {
        val paymentRequest = queue.poll() ?: return

        if (inFlightRequests.incrementAndGet() > parallelRequests) {
            inFlightRequests.decrementAndGet()
            queue.add(paymentRequest)
            return
        }

        if (!outgoingRateLimiter.tick()) {
            inFlightRequests.decrementAndGet()
            queue.add(paymentRequest)
            return
        }

        paymentExecutor.submit {
            try {
                if (now() < paymentRequest.deadline) {
                    if (now() + requestAverageProcessingTime.toMillis() > paymentRequest.deadline) {
                        theoreticalTimeout.increment()
                    }

                    paymentRequest.call.run()

                    if (now() > paymentRequest.deadline) {
                        actualTimeout.increment()
                    }
                } else {
                    actualTimeout.increment()
                }
            } finally {
                inFlightRequests.decrementAndGet()
            }
        }
    }


    private val releaseJob = scheduledExecutorScope.launch {
        while (true) {
            pollQueue()
        }
    }
}

public fun now() = System.currentTimeMillis()

data class PaymentRequest(
    val deadline: Long,
    val call: Runnable,
) : Comparable<PaymentRequest> {
    override fun compareTo(other: PaymentRequest): Int = deadline.compareTo(other.deadline)
}

class RetryManager(
    private val maxRetries: Int,
    private val baseDelayMillis: Long,
    private val backoffFactor: Double = 2.0,
    private val jitterMillis: Long = 50L
) {
    private var attempt = 0

    fun onFailure() {
        attempt++
        val delay = (baseDelayMillis * backoffFactor.pow(attempt.toDouble())).toLong()
        val jitter = Random.nextLong(0, jitterMillis + 1)
        val totalDelay = delay + jitter

        Thread.sleep(totalDelay.coerceAtMost(2000))
    }

    fun shouldRetry(currentTime: Long, deadline: Long): Boolean {
        if (currentTime >= deadline) return false
        if (attempt >= maxRetries) return false
        return true
    }
}
