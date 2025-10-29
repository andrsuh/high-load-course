package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.Semaphore;
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
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
    private val registry : MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val counter = Counter.builder("queries.amount").tag("name", "ordersAfter").register(registry)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    private val semaphoreToLimitParallelRequest = OngoingWindow(parallelRequests)
    private val slidingWindowRateLimiter =
        SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val retryAfterMillis: Long = (((requestAverageProcessingTime.toMillis().toDouble()) / 2.0) * (rateLimitPerSec / 7.0)).toLong().coerceAtLeast(500L)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        try {
            semaphoreToLimitParallelRequest.acquire()
            try {
                logger.warn("[$accountName] Submitting payment request for payment $paymentId")
                slidingWindowRateLimiter.tickBlocking()

                val transactionId = UUID.randomUUID()

                // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
                // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }

                logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

                var attempt = 1
                var success = false
                while (attempt <= 3 && !success) {
                    try {
                        val request = Request.Builder().run {
                            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                            post(emptyBody)
                        }.build()

                        client.newCall(request).execute().use { response ->
                            val body = try {
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
                        counter.increment()

                            if (body.result) {
                                success = true
                            } else if (attempt < 3) {
                                logger.warn("[$accountName] Retry #$attempt for payment $paymentId after ${retryAfterMillis}ms")
                                Thread.sleep(retryAfterMillis)
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
                        if (attempt < 3) {
                            Thread.sleep(retryAfterMillis)
                        }
                    }
                    attempt++
                }
            } finally {
                semaphoreToLimitParallelRequest.release()
            }
        } catch (e: InterruptedException) {
            logger.error("Problem while performing payment", e)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()