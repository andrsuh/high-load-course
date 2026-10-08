package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.PacedRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody: RequestBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .dispatcher(
            Dispatcher().apply {
                maxRequests = parallelRequests.coerceAtLeast(1)
                maxRequestsPerHost = parallelRequests.coerceAtLeast(1)
            }
        )
        .build()

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)
    private val rateLimiter = PacedRateLimiter(rateLimitPerSec)

    override fun name() = accountName

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun processingTime() = requestAverageProcessingTime

    override fun ratePerSecond() = rateLimitPerSec

    override fun tryReserve(deadline: Long): Boolean {
        if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
            return false
        }
        if (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
            return false
        }
        if (!rateLimiter.tick()) {
            ongoingWindow.releaseWindow()
            return false
        }
        return true
    }

    override fun release() {
        ongoingWindow.releaseWindow()
    }

    override fun submitPayment(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        onFinished: (Boolean) -> Unit,
    ) {
        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                complete(
                    paymentId = paymentId,
                    transactionId = transactionId,
                    result = false,
                    reason = e.message ?: e::class.java.simpleName,
                    onFinished = onFinished,
                )
            }

            override fun onResponse(call: Call, response: Response) {
                response.use { resp ->
                    val body = try {
                        mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Unparseable response for txId: $transactionId, payment: $paymentId, result code: ${resp.code}", e)
                        null
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body?.result}, message: ${body?.message}")

                    complete(
                        paymentId = paymentId,
                        transactionId = transactionId,
                        result = body?.result ?: false,
                        reason = body?.message ?: "Unparseable response with code ${resp.code}",
                        onFinished = onFinished,
                    )
                }
            }
        })
    }

    private fun complete(
        paymentId: UUID,
        transactionId: UUID,
        result: Boolean,
        reason: String?,
        onFinished: (Boolean) -> Unit,
    ) {
        try {
            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(paymentId) {
                it.logProcessing(result, now(), transactionId, reason = reason)
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Failed to log processing of payment $paymentId", e)
        } finally {
            ongoingWindow.releaseWindow()
            onFinished(result)
        }
    }
}

public fun now() = System.currentTimeMillis()
