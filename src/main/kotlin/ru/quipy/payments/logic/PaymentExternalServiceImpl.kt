package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.HdrHistogram.Histogram
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean


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

    private val client = OkHttpClient.Builder().build()
    private val semaphore = Semaphore(parallelRequests)
    private var currentTimeout85thPercentile = requestAverageProcessingTime.toMillis() * 2
    private var currentTimeout90thPercentile = requestAverageProcessingTime.toMillis() * 2
    private var currentTimeout95thPercentile = requestAverageProcessingTime.toMillis() * 2
    private val histogram = Histogram(1, requestAverageProcessingTime.toMillis() * 2, 2)

    private val rateLimiter = RateLimiter.of(
        "paymentRateLimiter-$accountName",
        RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .limitForPeriod(rateLimitPerSec)
            .timeoutDuration(Duration.ofSeconds(5))
            .build()
    )

    private fun createClientWithTimeout(timeout: Long): OkHttpClient {
        return client.newBuilder()
            .callTimeout(timeout, TimeUnit.MILLISECONDS)
            .build()
    }

    private val hedgedExecutor = Executors.newSingleThreadScheduledExecutor()

    private fun handleHedgeRequest(
        client: OkHttpClient, timeout: Long, request: Request, paymentId: UUID, transactionId: UUID, success: AtomicBoolean
    ): ScheduledFuture<*> {
        return hedgedExecutor.schedule({
            if (success.get()) return@schedule

            try {
                client.newCall(request).execute().use { response ->
                    if (response.isSuccessful) {
                        logger.info("[$accountName] Hedge request succeeded for txId: $transactionId")
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = "Hedge success for timeout $timeout")
                        }
                        success.set(true)
                    }
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Error in hedge request for txId: $transactionId, paymentId: $paymentId", e)
            }
        }, timeout, TimeUnit.MILLISECONDS)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        if (!rateLimiter.acquirePermission()) {
            return
        }

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
            addHeader("deadline", deadline.toString())
            addHeader("timeout", currentTimeout85thPercentile.toString())
        }.build()

        var retry = 0
        val maxRetries = 3
        var success = false
        val startTime = System.currentTimeMillis()

        semaphore.tryAcquire()
        try {
            while (retry < maxRetries && !success) {
                retry++

                if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                    logger.warn("[$accountName] PaymentId: $paymentId exceeds the deadline. Will process later.")

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), UUID.randomUUID(), reason = "Request timeout.")
                    }
                    return
                }

                val hedgedTimeout90th = histogram.getValueAtPercentile(90.0)
                val hedgedTimeout95th = histogram.getValueAtPercentile(95.0)

                val hedgedFuture90th = handleHedgeRequest(
                    createClientWithTimeout(hedgedTimeout90th),
                    hedgedTimeout90th,
                    request,
                    paymentId,
                    transactionId,
                    AtomicBoolean(success)
                )

                val hedgedFuture95th = handleHedgeRequest(
                    createClientWithTimeout(hedgedTimeout95th),
                    hedgedTimeout95th,
                    request,
                    paymentId,
                    transactionId,
                    AtomicBoolean(success)
                )

                createClientWithTimeout(currentTimeout85thPercentile).newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    val retryableResponseCodes = setOf(200, 400, 401, 403, 404, 405)

                    if (response.code !in retryableResponseCodes || !body.result) {
                        val delayDuration = response.headers["Retry-After"]?.let {
                            Duration.parse(it)
                        } ?: Duration.ofMillis(100)

                        Thread.sleep(delayDuration.toMillis())
                        return@use
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(true, now(), transactionId, reason = body.message)
                    }
                    success = true
                    hedgedFuture90th.cancel(false)
                    hedgedFuture95th.cancel(false)
                }

                val duration = System.currentTimeMillis() - startTime
                histogram.recordValue(duration)

                currentTimeout90thPercentile = histogram.getValueAtPercentile(90.0)
                currentTimeout95thPercentile = histogram.getValueAtPercentile(95.0)
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
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()