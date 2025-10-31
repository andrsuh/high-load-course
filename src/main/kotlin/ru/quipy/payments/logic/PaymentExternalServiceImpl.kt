package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.apigateway.TooManyRequestsError
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
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
    private val parallelRequests = properties.parallelRequests

    private val semaphore = Semaphore(parallelRequests)

    private val minBudgetForAttempt = 150L
    private val safetyMarginMs = 50L

    private val client = OkHttpClient.Builder()
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(30, TimeUnit.SECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val windowControl = OngoingWindow(parallelRequests);

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId with deadline: ${deadline - now()}ms remaining")

        fun remainingTime(): Long = deadline - now()

        val acquired = try {
            semaphore.tryAcquire(remainingTime(), TimeUnit.MILLISECONDS)
        } catch (e: Exception) {
            false
        }

        if (!acquired) {
            val transactionId = UUID.randomUUID()
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                it.logProcessing(false, now(), transactionId, reason = "Window slot acquisition timeout")
            }
            return
        }

        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        try {
            if (remainingTime() < minBudgetForAttempt * 2) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Insufficient time for rate limiting")
                }
                return
            }
            if (!rateLimiter.tick()) {
                if (remainingTime() > minBudgetForAttempt * 2) {
                    semaphore.release()
                    Thread.sleep(5)
                    performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                    return
                } else {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Rate limit exceeded")
                    }
                    return
                }
            }

            val callTimeoutMs = remainingTime() - safetyMarginMs
            if (callTimeoutMs <= 0) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "No time budget for API call")
                }
                return
            }

            val callClient = client.newBuilder()
                .callTimeout(callTimeoutMs, TimeUnit.MILLISECONDS)
                .readTimeout(callTimeoutMs, TimeUnit.MILLISECONDS)
                .writeTimeout(callTimeoutMs, TimeUnit.MILLISECONDS)
                .connectTimeout(minOf(callTimeoutMs, 1000), TimeUnit.MILLISECONDS)
                .build()

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            callClient.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                val shouldRetry = when {
                    response.code == 429 -> true
                    response.code in 500..599 -> true
                    response.code == 200 && !body.result -> true
                    else -> false
                }

                if (shouldRetry && remainingTime() > minBudgetForAttempt * 3) {
                    val backoffTime = when {
                        response.code == 429 -> 100L
                        else -> 50L
                    }

                    logger.info("[$accountName] Retrying payment $paymentId due to temporary error: ${response.code}")
                    semaphore.release()
                    performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                    return
                }

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

            val reason = when (e) {
                is SocketTimeoutException -> "Request timeout"
                else -> e.message ?: "Unknown error"
            }

            val shouldRetry = e is SocketTimeoutException && remainingTime() > minBudgetForAttempt * 2

            if (shouldRetry) {
                logger.info("[$accountName] Retrying payment $paymentId due to timeout")
                semaphore.release()
                performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                return
            }

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = reason)
            }
        } finally {
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()