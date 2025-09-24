package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    private val windowRateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val host = parseHost(paymentProviderHostPort)
    private val port = parsePort(paymentProviderHostPort)
    private val baseUrlComponents = mapOf(
        "serviceName" to serviceName,
        "token" to token,
        "accountName" to accountName
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn(
            "[{}] Submitting payment request for payment {}",
            accountName,
            paymentId
        )

        val transactionId = UUID.randomUUID()

        windowRateLimiter.tickBlocking()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info(
            "[{}] Submit: {} , txId: {}",
            accountName,
            paymentId,
            transactionId
        )

        try {
            val request = Request.Builder().run {
                val url = HttpUrl.Builder()
                    .scheme("http")
                    .host(host)
                    .port(port)
                    .addPathSegments("external/process")
                    .apply {
                        baseUrlComponents.forEach { (key, value) -> addQueryParameter(key, value) }
                        addQueryParameter("transactionId", transactionId.toString())
                        addQueryParameter("paymentId", paymentId.toString())
                        addQueryParameter("amount", amount.toString())
                    }
                    .build()

                url(url).
                post(emptyBody)
            }.build()

            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error(
                        "[{}] [ERROR] Payment processed for txId: {}, payment: {}, result code: {}, reason: {}",
                        accountName,
                        transactionId,
                        paymentId,
                        response.code,
                        response.body?.string()
                    )
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn(
                    "[{}] Payment processed for txId: {}, payment: {}, succeeded: {}, message: {}",
                    accountName,
                    transactionId,
                    paymentId,
                    body.result,
                    body.message
                )

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: SocketTimeoutException) {
            logger.error(
                "[{}] Payment timeout for txId: {}, payment: {}",
                accountName,
                transactionId,
                paymentId,
                e
            )
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
        } catch (e: Exception) {
            logger.error(
                "[{}] Payment failed for txId: {}, payment: {}",
                accountName,
                transactionId,
                paymentId,
                e
            )
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
        }
    }

    private fun parseHost(hostPort: String): String {
        val parts = hostPort.split(":")
        return if (parts.size == 2) {
            parts[0]
        } else {
            hostPort
        }
    }

    private fun parsePort(hostPort: String): Int {
        val parts = hostPort.split(":")
        return if (parts.size == 2) {
            parts[1].toInt()
        } else {
            80
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)!!

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
    }
}

fun now() = System.currentTimeMillis()
