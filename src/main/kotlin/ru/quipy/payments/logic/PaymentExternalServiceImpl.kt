package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob

import org.springframework.stereotype.Component


import kotlinx.coroutines.launch

import io.micrometer.core.annotation.Timed
import io.micrometer.core.annotation.Counted
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.CountingRateLimiter
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


class RateLimitExceededException(val retryAfter: Long) : Exception("Rate limit exceeded")

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
    ) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private class PaymentTask(
        val paymentId: UUID,
        val amount: Int,
        val paymentStartedAt: Long,
        val deadline: Long,
        val transactionId: UUID,
        val sample: Timer.Sample
    ) {
        fun deadlineExpired() : Boolean {
            return System.currentTimeMillis() >= deadline
        }
    }

    private val paymentQueue = Channel<PaymentTask>(Channel.UNLIMITED)
    private val paymentQueueSize = AtomicInteger(0)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestFailuredCounter = Counter.builder("payment_request_failed_count").description("Number of payments request failed").register(meterRegistry)
    private val submissionCounter = Counter.builder("payment_submission_count").description("Number of payments submitted").register(meterRegistry)
    private val processingTimer = Timer.builder("payment_processing_time").description("Time taken to process a payment").register(meterRegistry)
    private val fullProcessingTimer = Timer.builder("payment_full_processing_time").description("Time taken to full process payment").register(meterRegistry)
    private val successfulProcessingCounter = Counter.builder("payment_successful_processing_count").description("Number of payments successfully processed").register(meterRegistry)

    init {
        setupMetrics()
    }

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)
    private val rateLimiterOneSecond = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val client = OkHttpClient.Builder().build()

    private fun setupMetrics() {
        Gauge.builder("payment_queue_size", paymentQueueSize){it -> it.get().toDouble()}.description("Size of the payment queue").register(meterRegistry)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        if (!rateLimiterOneSecond.tick()) {
            throw RateLimitExceededException(requestAverageProcessingTime.toSeconds())
        }
        
        submissionCounter.increment()
        paymentQueueSize.incrementAndGet()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
        processPaymentTask(paymentId, amount, paymentStartedAt, deadline, transactionId, Timer.start())
    }

    private fun processPaymentTask(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, transactionId: UUID, sampleFullProcessing: Timer.Sample) {
        val sample = Timer.start()
        logger.info("[$accountName] Processing payment request for payment $paymentId, txId: $transactionId")
        var needToReleaseWindow: Boolean = false
        try {
            val low_bound_of_timeout = deadline - now() - requestAverageProcessingTime.toMillis()
            if (low_bound_of_timeout <= 0) {
                requestFailuredCounter.increment()
                logger.warn("[$accountName] Request timeout for payment $paymentId, txId: $transactionId")
                throw Exception("Request timeout")
            }
            needToReleaseWindow = true

            ongoingWindow.putIntoWindow()
            logger.info("[$accountName] Acquired semaphore and rate limiter for payment $paymentId, txId: $transactionId")
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val clientWithTimeout = client
                    .newBuilder()
                    .callTimeout(Duration.ofMillis(deadline - now()))
                    .build()

            clientWithTimeout.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                successfulProcessingCounter.increment()
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
        } finally {
            if (needToReleaseWindow) {
                ongoingWindow.releaseWindow()
            }
            paymentQueueSize.decrementAndGet()
            sample.stop(processingTimer)
            sampleFullProcessing.stop(fullProcessingTimer)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()