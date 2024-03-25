package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    properties: ExternalServiceProperties,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private var next: PaymentExternalService? = null
    private val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = RateLimiter(rateLimitPerSec)
    private val ongoingWindow = OngoingWindow(parallelRequests)

    fun setNext(next: PaymentExternalService) {
        this.next = next
    }

    private fun cancelPayment(paymentId: UUID, paymentStartedAt: Long) {
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Canceled.")
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        if (Duration.ofMillis(now() - paymentStartedAt) + requestAverageProcessingTime > paymentOperationTimeout) {
            logger.warn("[$accountName] Cannot process request due to SLA")
            if (next == null) {
                cancelPayment(paymentId, paymentStartedAt)
            } else {
                next!!.submitPaymentRequest(paymentId, amount, paymentStartedAt)
            }
            return
        }

        if (!ongoingWindow.tryAcquire()) {
            logger.warn("[$accountName] Reached limit of parallel requests")
            if (next == null) {
                cancelPayment(paymentId, paymentStartedAt)
            } else {
                next!!.submitPaymentRequest(paymentId, amount, paymentStartedAt)
            }
            return
        }

        if (!rateLimiter.tick()) {
            ongoingWindow.release()
            logger.warn("[$accountName] Reached limit of RPS")
            if (next == null) {
                cancelPayment(paymentId, paymentStartedAt)
            } else {
                next!!.submitPaymentRequest(paymentId, amount, paymentStartedAt)
            }
            return
        }

        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        OkHttpClient.Builder().run {
            callTimeout(paymentOperationTimeout - Duration.ofMillis(now() - paymentStartedAt))
            build()
        }.newCall(request).enqueue(
            object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    ongoingWindow.release()
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    when (e) {
                        is SocketTimeoutException -> {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    ongoingWindow.release()
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            }
        )
    }
}

public fun now() = System.currentTimeMillis()
