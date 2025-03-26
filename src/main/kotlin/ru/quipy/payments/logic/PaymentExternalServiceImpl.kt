package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.FixedWindowRateLimiter
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
    private val connectionTimeout = requestAverageProcessingTime
//        .plusMillis(requestAverageProcessingTime.toMillis() / 2)

    private val client = OkHttpClient.Builder()
        .connectTimeout(connectionTimeout)
        .readTimeout(connectionTimeout)
        .callTimeout(connectionTimeout)
        .writeTimeout(connectionTimeout)
        .build()

    private val semaphore = Semaphore(parallelRequests)
    private val inputLimiter = FixedWindowRateLimiter((parallelRequests * 1000L /  requestAverageProcessingTime.toMillis()).toInt(), 1000L, TimeUnit.MILLISECONDS)
    private val rateLimiter = FixedWindowRateLimiter(rateLimitPerSec, 1000L, TimeUnit.MILLISECONDS)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        inputLimiter.tickBlocking()
        if (deadline < now() + requestAverageProcessingTime.toMillis()) {
            logger.info("UwU")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return
        }
        semaphore.acquire()
        rateLimiter.tickBlocking()
        if (deadline < now() + requestAverageProcessingTime.toMillis()) {
            semaphore.release()
            logger.info("Release because of deadline. Semaphore queue length: ${semaphore.queueLength}")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return
        }

        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")
        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        try {
            val startTime = now()
            client.newCall(request).execute().use { response ->
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
                logger.info("completed, time = ${startTime - now()}ms")

                semaphore.release()
                logger.info("Release. Semaphore queue length: ${semaphore.queueLength}")
                if (!body.result) {
                    performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    semaphore.release()
                    logger.info("Release. Semaphore queue length: ${semaphore.queueLength}")
                    performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    semaphore.release()
                    logger.info("Release. Semaphore queue length: ${semaphore.queueLength}")
                    performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()