package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import kotlin.math.pow


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

    private val semaphore = java.util.concurrent.Semaphore(properties.parallelRequests)
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val paymentRequestsCounter = Counter.builder("payment.requests.incoming")
        .description("Total payment requests received by adapter")
        .tag("adapter", "payment")
        .register(Metrics.globalRegistry)

    private val paymentSuccessCounter = Counter.builder("payment.requests.processed")
        .description("Total payment requests successfully processed")
        .tag("outcome", "success")
        .register(Metrics.globalRegistry)

    private val paymentErrorCounter = Counter.builder("payment.requests.processed")
        .description("Total payment requests failed")
        .tag("outcome", "error")
        .register(Metrics.globalRegistry)

    private val client = OkHttpClient.Builder().build()
    val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val maxAttempts = 4
    private val maxDelayMs = 2000L
    private val delayBaseMs = 200L

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var attempt = 0
        var currentDelay = delayBaseMs

        while (attempt < maxAttempts) {
            attempt++

            val currentTime = System.currentTimeMillis()
            if (currentTime > deadline) {
                paymentErrorCounter.increment()
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline expired after $attempt attempts)")
                }
                return
            }

            try {
                var success = false
                try {
                    slidingWindowRateLimiter.tickBlocking()

                    val request = Request.Builder().run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                    }.build()

                    semaphore.acquire()
                    try {
                        client.newCall(request).execute().use { response ->
                            val body = try {
                                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                            paymentRequestsCounter.increment()
                            if (body.result) {
                                paymentSuccessCounter.increment()
                                paymentESService.update(paymentId) {
                                    it.logProcessing(true, now(), transactionId, reason = body.message)
                                }
                                success = true
                            } else {
                                paymentErrorCounter.increment()

                                if (!(response.code == 429 || response.code in 500..504)) {
                                    paymentESService.update(paymentId) {
                                        it.logProcessing(false, now(), transactionId, reason = body.message)
                                    }
                                }
                                success = false
                            }
                        }
                    } finally {
                        semaphore.release()
                    }
                } catch (e: Exception) {
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            success = false
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                            paymentErrorCounter.increment()
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Non-retriable exception: ${e.message}")
                            }
                            success = false
                        }
                    }
                }

                if (success) return

                if (attempt == maxAttempts) {
                    paymentErrorCounter.increment()
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "All $maxAttempts attempts failed")
                    }
                    return
                }

                currentDelay = exponentialBackoffDelay(attempt)
                if (currentDelay <= 0) {
                    paymentErrorCounter.increment()
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "No time for retry delay")
                    }
                    return
                }

                Thread.sleep(currentDelay)
            } catch (e: Exception) {
                paymentErrorCounter.increment()
            }
        }
    }

    private fun exponentialBackoffDelay(attempt: Int): Long {
        return minOf((delayBaseMs * 2.0.pow((attempt - 1).toDouble())).toLong(), maxDelayMs)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()