package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.PaymentTimeoutProperties
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
    private val timeouts: PaymentTimeoutProperties,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        val deadlineGraceMs = 200L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val callTimer = Timer.builder("payment.external.call")
        .publishPercentiles(0.5, 0.8, 0.99)
        .tag("account", accountName)
        .register(meterRegistry)

    private val accountTimeout: Duration = timeouts.timeout[accountName]
        ?: properties.averageProcessingTime.multipliedBy(2)

    private val client = OkHttpClient.Builder()
        .connectTimeout(1, TimeUnit.SECONDS)
        .readTimeout(accountTimeout.toMillis(), TimeUnit.MILLISECONDS)
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var lastBody: ExternalSysResponse? = null
        var lastFailure: String? = null
        var retryReason = "unknown"
        var retryBackoffMs = -1L

        for (attempt in 1..timeouts.maxAttempts) {
            if (attempt > 1) {
                meterRegistry.counter("payment.external.retries", "account", accountName, "reason", retryReason)
                    .increment()
            }

            if (deadline - now() - deadlineGraceMs <= 0) {
                lastFailure = "Deadline exceeded, dropping retry"
                break
            }

            val remainingMs = deadline - now() - deadlineGraceMs
            val callTimeoutMs = minOf(accountTimeout.toMillis(), remainingMs).coerceIn(100L, 100_000L)
            val call = client.newCall(
                Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    header("Accept", "application/json")
                    post(emptyBody)
                }.build()
            )
            call.timeout().timeout(callTimeoutMs, TimeUnit.MILLISECONDS)

            val attemptStartedAt = now()
            try {
                call.execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        lastFailure = "Malformed response (HTTP ${response.code}): ${e.message}"
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${e.message}")
                        null
                    }

                    if (body != null) {
                        lastBody = body
                        if (body.result) {
                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: true")
                            break
                        } else {
                            lastFailure = "External system reported failure: ${body.message}"
                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: false, message: ${body.message}")
                            if (attempt < timeouts.maxAttempts) {
                                logger.warn("[$accountName] Retrying business-level failure, attempt $attempt/${timeouts.maxAttempts}")
                                retryReason = "business_fail"
                                retryBackoffMs = backoffMs(attempt, 0, deadline)
                            }
                        }
                    } else if (response.isSuccessful) {
                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: false")
                        break
                    } else {
                        val code = response.code
                        val retryAfter = response.header("Retry-After")?.toLongOrNull() ?: 0L
                        lastFailure = "HTTP $code"
                        if (!isRetryable(code)) {
                            logger.error("[$accountName] Non-retryable HTTP $code for txId: $transactionId, payment: $paymentId")
                            break
                        }
                        logger.warn("[$accountName] HTTP $code for txId: $transactionId, payment: $paymentId, attempt $attempt/${timeouts.maxAttempts}, retrying")
                        retryReason = if (code == 429) "http_429" else "http_5xx"
                        retryBackoffMs = backoffMs(attempt, retryAfter, deadline)
                    }
                }
            } catch (e: SocketTimeoutException) {
                retryReason = "timeout"
                lastFailure = "Request timeout: ${e.message}"
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt $attempt/${timeouts.maxAttempts}", e)
                retryBackoffMs = backoffMs(attempt, 0, deadline)
            } catch (e: InterruptedIOException) {
                retryReason = "timeout"
                lastFailure = "Request timeout: ${e.message}"
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt $attempt/${timeouts.maxAttempts}", e)
                retryBackoffMs = backoffMs(attempt, 0, deadline)
            } catch (e: Exception) {
                retryReason = "error"
                lastFailure = e.message ?: e.javaClass.simpleName
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, attempt $attempt/${timeouts.maxAttempts}", e)
                retryBackoffMs = backoffMs(attempt, 0, deadline)
            } finally {
                callTimer.record(Duration.ofMillis(now() - attemptStartedAt))
            }

            if (retryBackoffMs >= 0) {
                sleep(retryBackoffMs)
                retryBackoffMs = -1L
                continue
            }
        }

        val finalBody = lastBody
        paymentESService.update(paymentId) {
            if (finalBody != null) {
                it.logProcessing(finalBody.result, now(), transactionId, reason = finalBody.message)
            } else {
                it.logProcessing(false, now(), transactionId, reason = lastFailure ?: "Unknown error")
            }
        }
    }

    private fun isRetryable(code: Int) = code == 429 || code >= 500

    private fun backoffMs(attempt: Int, retryAfter: Long, deadline: Long): Long {
        val exp = timeouts.baseDelayMs * (1L shl (attempt - 1))
        val jitter = ThreadLocalRandom.current().nextLong(0, 150)
        val raw = if (retryAfter > 0) retryAfter * 1000 else minOf(maxOf(exp, timeouts.baseDelayMs), timeouts.maxDelayMs) + jitter
        val budget = (deadline - now() - deadlineGraceMs).coerceAtLeast(0L)
        return minOf(raw, budget)
    }

    private fun sleep(ms: Long) {
        if (ms <= 0) return
        try {
            Thread.sleep(ms)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()