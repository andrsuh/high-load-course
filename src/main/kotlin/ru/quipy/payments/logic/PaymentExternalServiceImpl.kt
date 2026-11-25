package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.io.InterruptedIOException
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
    private val timeoutMillis: Long,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .connectTimeout(1000, TimeUnit.MILLISECONDS)
        .readTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
        .writeTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
        .callTimeout(timeoutMillis, TimeUnit.MILLISECONDS).build()

    private val paymentLimiter =
        SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))

    private val semaphore = Semaphore(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): PaymentAttempt {
        val transactionId = UUID.randomUUID()
        var result = PaymentAttempt(false)

        val now = System.currentTimeMillis()
        val remainingTime = maxOf(0, deadline - now)

        val acquired = try {
            if (remainingTime > 0) {
                semaphore.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)
            } else {
                false
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            logger.warn("[$accountName] Interrupted while waiting for semaphore for payment $paymentId")
            return PaymentAttempt(false)
        }

        if (!acquired) {
            logger.warn("[$accountName] Could not acquire semaphore within deadline for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Semaphore acquisition timeout")
            }
            return PaymentAttempt(false)
        }
        try {
            val now = System.currentTimeMillis()
            val deadlineTimeout = maxOf(0, deadline - now)
            if (!paymentLimiter.tickBlocking(Duration.ofMillis(deadlineTimeout))) {
                logger.warn("[$accountName] wait before payment for $paymentId")
                return PaymentAttempt(false)
            }

            logger.info("[$accountName] Submitting payment request for payment $paymentId")

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val remaining = deadline - now()
            if (remaining <= 0) return PaymentAttempt(false)
            val call = client.newCall(request)
            call.timeout().timeout(minOf(timeoutMillis, remaining), TimeUnit.MILLISECONDS)
            val started = System.nanoTime()
            var status = "error"
            var outcomeReason: String? = null
            try {
                call.execute().use { response ->
                    status = response.code.toString()
                    val rawBody = response.body?.string()
                    val body = try {
                        mapper.readValue(rawBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${rawBody}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    result = PaymentAttempt(body.result, !body.result && response.code in listOf(408, 429, 500, 502, 503, 504))

                    outcomeReason = body.message
                }
            } catch (e: InterruptedIOException) {
                status = "timeout"
                result = PaymentAttempt(false, true)
                throw e
            } finally {
                Timer.builder("payment_http_duration")
                    .description("External HTTP call duration, excluding local queues")
                    .tags("account", accountName, "status", status)
                    .publishPercentiles(0.5, 0.8, 0.95, 0.99)
                    .register(meterRegistry)
                    .record(System.nanoTime() - started, TimeUnit.NANOSECONDS)
            }
            paymentESService.update(paymentId) {
                it.logProcessing(result.success, now(), transactionId, reason = outcomeReason)
            }
        } catch (e: Exception) {
            when (e) {
                is InterruptedIOException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            semaphore.release()
        }

        return result
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()