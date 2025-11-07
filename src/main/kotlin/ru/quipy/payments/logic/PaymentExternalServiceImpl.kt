package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
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
import java.util.concurrent.TimeUnit
import kotlin.math.pow


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry
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

    private val client = OkHttpClient.Builder().callTimeout(requestAverageProcessingTime.toMillis() * 2, TimeUnit.MILLISECONDS).build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val submittedCounter = Counter.builder("payments_submitted_total").register(meterRegistry)
    private val sentQueriesSuccess = Counter.builder("payments_success").register(meterRegistry)

    private val requestLatency = DistributionSummary.builder("request_latency")
        .description("Request latency.")
        .publishPercentiles(0.5, 0.8, 0.90, 0.95, 0.99)
        .register(meterRegistry)


    private fun waitRateLimitOrTimeout(deadline: Long): Boolean {
        while (!rateLimiter.tick()) {
            if (now() >= deadline) return false
            Thread.sleep(5)
        }
        return true
    }

    private val retryCount = 4
    private val maxDelay = 2000L
    private val baseDelay = 200L

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        submittedCounter.increment()
        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        ongoingWindow.acquire()
        if (!waitRateLimitOrTimeout(deadline)) {
            logger.error("[$accountName] Rate limit wait exceeded deadline for txId: $transactionId, payment: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit wait exceeded deadline.")
            }
            return
        }

        try {

            var x = 0
            while (x < retryCount) {
                x++
                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                val start = System.currentTimeMillis()
                client.newCall(request).execute().use { response ->
                    val latency = (System.currentTimeMillis() - start).toDouble()
                    requestLatency.record(latency)


                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    ongoingWindow.release()

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    if (body.result) {
                        sentQueriesSuccess.increment()
                    }
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    if (body.result || (x == retryCount))
                        break
                    Thread.sleep(exponentialBackoffDelay(x))
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

    private fun exponentialBackoffDelay(attempt: Int): Long {
        return minOf((baseDelay * 2.0.pow((attempt - 1).toDouble())).toLong(), maxDelay)
    }
}

public fun now() = System.currentTimeMillis()