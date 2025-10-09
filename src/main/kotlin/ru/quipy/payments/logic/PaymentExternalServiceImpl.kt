package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.ConnectionPool
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread


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

    private val client = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(100, 5, TimeUnit.MINUTES))
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(10, TimeUnit.SECONDS)
        .build()

    private val parallelRequestSemaphore = Semaphore(parallelRequests)

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = (rateLimitPerSec * 1.1).toLong(),
        window = Duration.ofSeconds(1)
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        thread {
            executeWithSemaphore(paymentId, transactionId, amount)
        }
    }

    private fun executeWithSemaphore(paymentId: UUID, transactionId: UUID, amount: Int) {
        try {
            parallelRequestSemaphore.acquire()
            logger.debug("[$accountName] Acquired semaphore for payment $paymentId, available permits: ${parallelRequestSemaphore.availablePermits()}")

            var currentAttempt = 1
            while (!rateLimiter.tick()) {
                logger.debug("[$accountName] Rate limit hit for payment $paymentId, attempt $currentAttempt, micro-sleep...")
                Thread.sleep(5)
                currentAttempt++
                if (currentAttempt % 100 == 0) {
                    logger.info("[$accountName] Still waiting for rate limit for payment $paymentId, attempt $currentAttempt")
                }
            }

            executePaymentRequest(paymentId, transactionId, amount)
        } catch (e: InterruptedException) {
            logger.error("[$accountName] Interrupted while waiting for semaphore for payment $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Interrupted waiting for parallel slot")
            }
        } finally {
            parallelRequestSemaphore.release()
            logger.debug("[$accountName] Released semaphore for payment $paymentId")
        }
    }

    private fun executePaymentRequest(paymentId: UUID, transactionId: UUID, amount: Int, attempt: Int = 1) {
        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            client.newCall(request).execute().use { response ->
                if (response.code == 429 && attempt <= 10) {
                    logger.warn("[$accountName] Received 429 for payment $paymentId, attempt $attempt, retrying...")
                    Thread.sleep(20)
                    executePaymentRequest(paymentId, transactionId, amount, attempt + 1)
                    return
                }

                if (!response.isSuccessful && attempt <= 3) {
                    logger.warn("[$accountName] Non-successful response ${response.code} for payment $paymentId, attempt $attempt, retrying...")
                    Thread.sleep(50)
                    executePaymentRequest(paymentId, transactionId, amount, attempt + 1)
                    return
                }

                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    if (attempt <= 3) {
                        logger.warn("[$accountName] Timeout for payment $paymentId, attempt $attempt, retrying...")
                        val delayMs = minOf(25 + (attempt * 10), 80).toLong()
                        Thread.sleep(delayMs)
                        executePaymentRequest(paymentId, transactionId, amount, attempt + 1)
                        return
                    }
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId after $attempt attempts", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout after retries.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()