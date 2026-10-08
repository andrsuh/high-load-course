package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.PacedRateLimiter
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
    private val rateLimiter = PacedRateLimiter(rateLimitPerSec)
    private val paymentSlots = Semaphore(parallelRequests.also {
        require(it > 0) { "Parallel requests must be positive" }
    }, true)

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        val submissionDeadline = deadline - requestAverageProcessingTime.toMillis()
        val acquired = try {
            val remaining = submissionDeadline - now()
            remaining > 0 && paymentSlots.tryAcquire(remaining, TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            rejectPayment(paymentId, transactionId, paymentStartedAt, "Interrupted while waiting for payment slot.")
            return
        }
        if (!acquired) {
            rejectPayment(paymentId, transactionId, paymentStartedAt, "Not enough time to wait for payment slot and process payment.")
            return
        }

        try {
            performPaymentWithSlot(paymentId, amount, paymentStartedAt, deadline, submissionDeadline, transactionId)
        } finally {
            paymentSlots.release()
        }
    }

    private fun rejectPayment(paymentId: UUID, transactionId: UUID, paymentStartedAt: Long, reason: String) {
        paymentESService.update(paymentId) {
            it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    private fun performPaymentWithSlot(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        submissionDeadline: Long,
        transactionId: UUID,
    ) {

        val rejectionReason = try {
            if (rateLimiter.acquire(submissionDeadline)) null else "Not enough time to wait for rate limit and process payment."
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            "Interrupted while waiting for rate limit permission."
        }
        if (rejectionReason != null) {
            rejectPayment(paymentId, transactionId, paymentStartedAt, rejectionReason)
            return
        }

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val remaining = deadline - now()
            if (remaining <= 0) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Payment deadline exceeded before HTTP call.")
                }
                return
            }
            val call = client.newCall(request)
            call.timeout().timeout(remaining, TimeUnit.MILLISECONDS)
            call.execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] Invalid payment response for txId: $transactionId, payment: $paymentId, result code: ${response.code}")
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
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
