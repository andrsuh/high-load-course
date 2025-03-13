package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.sync.Semaphore
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CustomRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.lang.Long.min
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*


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
    private val parallelRequests = properties.parallelRequests
    private val maxRetries = properties.maxRetries ?: 3 // Add default value if not provided
    private val retryDelay = properties.retryDelay ?: Duration.ofMillis(500) // Add default value if not provided

    private val client = OkHttpClient.Builder().build()

    private val rateLimiter = CustomRateLimiter(
        min(rateLimitPerSec.toLong() * requestAverageProcessingTime.toMillis() / 1100, parallelRequests.toLong()),
        requestAverageProcessingTime,
    )
    private val semaphore = Semaphore(parallelRequests)

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        semaphore.acquire()
        val timeBeforeDeadline = deadline - now() - requestAverageProcessingTime.toMillis() * 2
        if (timeBeforeDeadline <= 0) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request will cause timeout, stopped")
            }
            semaphore.release()
            return
        }

        if (!rateLimiter.tickBlocking(deadline + requestAverageProcessingTime.toMillis())) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request will cause timeout, stopped")
            }
            semaphore.release()
            return
        }

        try {
            var attempt = 0
            lateinit var body: ExternalSysResponse

            do {
                attempt++

                val request = Request.Builder().run {
                    url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                try {
                    client.newCall(request).execute().use { response ->
                        body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }

                        when (response.code) {
                            400, 401, 403, 404, 405 -> {
                                throw RuntimeException("Client error code: ${response.code}")
                            }
                            else -> { /* no op */ }
                        }
                    }
                } catch (e: SocketTimeoutException) {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                    body = ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Request timeout.")
                } catch (e: Exception) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                    body = ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    throw e
                }

                if (!body.result && attempt < maxRetries) {
                    logger.warn("[$accountName] Retrying payment request for txId: $transactionId, attempt $attempt/$maxRetries")
                    val newRetry: Duration = retryDelay.multipliedBy(attempt.toLong())
                    Thread.sleep(newRetry.toMillis())
                }
            } while (!body.result && attempt < maxRetries)
        } finally {
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()