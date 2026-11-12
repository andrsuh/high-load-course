package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
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
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        val maxAttempts = 4
        val baseDelayMs = 200L
        val maxDelayMs = 800L
        val deadlineGraceMs = 200L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val client = OkHttpClient.Builder()
        .connectTimeout(1, TimeUnit.SECONDS)
        .readTimeout(10, TimeUnit.SECONDS)
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

        for (attempt in 1..maxAttempts) {
            if (deadline - now() - deadlineGraceMs <= 0) {
                lastFailure = "Deadline exceeded, dropping retry"
                break
            }

            val remainingMs = deadline - now() - deadlineGraceMs
            val callTimeoutMs = remainingMs.coerceIn(100L, 100_000L)
            val call = client.newCall(
                Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    header("Accept", "application/json")
                    post(emptyBody)
                }.build()
            )
            call.timeout().timeout(callTimeoutMs, TimeUnit.MILLISECONDS)

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
                            if (attempt < maxAttempts) {
                                logger.warn("[$accountName] Retrying business-level failure, attempt $attempt/$maxAttempts")
                                sleep(backoffMs(attempt, 0, deadline))
                                continue
                            }
                            break
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
                        logger.warn("[$accountName] HTTP $code for txId: $transactionId, payment: $paymentId, attempt $attempt/$maxAttempts, retrying")
                        sleep(backoffMs(attempt, retryAfter, deadline))
                        continue
                    }
                }
            } catch (e: SocketTimeoutException) {
                lastFailure = "Request timeout: ${e.message}"
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt $attempt/$maxAttempts", e)
                sleep(backoffMs(attempt, 0, deadline))
            } catch (e: Exception) {
                lastFailure = e.message ?: e.javaClass.simpleName
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, attempt $attempt/$maxAttempts", e)
                sleep(backoffMs(attempt, 0, deadline))
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
        val exp = baseDelayMs * (1L shl (attempt - 1))
        val jitter = ThreadLocalRandom.current().nextLong(0, 150)
        val raw = if (retryAfter > 0) retryAfter * 1000 else minOf(maxOf(exp, baseDelayMs), maxDelayMs) + jitter
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
    override fun rpc() = properties.rateLimitPerSec.toDouble()
    override fun rapt() = properties.averageProcessingTime
    override fun pr() = properties.parallelRequests.toDouble()
}

public fun now() = System.currentTimeMillis()