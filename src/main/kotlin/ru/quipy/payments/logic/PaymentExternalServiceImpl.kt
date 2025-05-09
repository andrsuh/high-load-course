package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.abs
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    // private val parallelRequests = properties.parallelRequests
    private val parallelRequests = 30 // Увеличенное значение для параллельных запросов

    private val requestQueue = ConcurrentLinkedQueue<PaymentRequest>()
    private val executor = Executors.newFixedThreadPool(parallelRequests)

    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000))

    private val client = OkHttpClient.Builder()
        .callTimeout((requestAverageProcessingTime.toMillis() * 1.1).toLong(), TimeUnit.MILLISECONDS)
        .build()

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Adding payment request for payment $paymentId to queue")
        requestQueue.add(PaymentRequest(paymentId, amount, paymentStartedAt, deadline))
        processQueue()
    }

    private fun processQueue() {
        while (requestQueue.isNotEmpty()) {
            val request = requestQueue.poll() ?: return
            executor.submit {
                executePayment(request)
            }
        }
    }

    private fun executePayment(request: PaymentRequest) {
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Processing payment for ${request.paymentId}, txId: $transactionId")

        paymentESService.update(request.paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - request.paymentStartedAt))
        }

        var attempt = 0
        val maxRetries = 5
        var delay = 1000L

        while (attempt < maxRetries) {
            limiter.tickBlocking()
            val httpRequest = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=${request.paymentId}&amount=${request.amount}")
                post(emptyBody)
            }.build()

            try {
                client.newCall(httpRequest).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: ${request.paymentId}, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), request.paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: ${request.paymentId}, succeeded: ${body.result}, message: ${body.message}")

                    if (body.result) {
                        paymentESService.update(request.paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        return
                    }

                    when (HttpStatus.valueOf(response.code)) {
                        HttpStatus.BAD_REQUEST,
                        HttpStatus.UNAUTHORIZED,
                        HttpStatus.FORBIDDEN,
                        HttpStatus.NOT_FOUND,
                        HttpStatus.METHOD_NOT_ALLOWED -> {
                            logger.error("[$accountName] Payment failed permanently for txId: $transactionId, code: ${response.code}")
                            return
                        }
                        HttpStatus.TOO_MANY_REQUESTS -> {
                            delay *= 2
                        }
                        // HttpStatus.TOO_MANY_REQUESTS,
                        HttpStatus.SERVICE_UNAVAILABLE -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, code: ${response.code}")
                            val retryAfter = response.headers["Retry-After"]?.toLongOrNull()
                            retryAfter?.let {
                                delay = retryAfter * 1000
                            }
                        }
                        HttpStatus.INTERNAL_SERVER_ERROR -> {
                            delay = 0
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, code: ${response.code}")
                        }
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: ${request.paymentId}", e)
                        paymentESService.update(request.paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: ${request.paymentId}", e)
                        paymentESService.update(request.paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                processQueue()
            }

            attempt++
            if (attempt < maxRetries) {
                val jitter = (delay * 0.3 * Math.random()).toLong()
                val finalDelay = min(delay + jitter, abs(request.deadline - now()))

                val adjustedDelay = finalDelay + jitter

                logger.info("Retrying in ${adjustedDelay}ms (attempt ${attempt + 1})")
                Thread.sleep(adjustedDelay)
            }
        }

        paymentESService.update(request.paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Max retries reached")
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

private data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long
)

fun now() = System.currentTimeMillis()