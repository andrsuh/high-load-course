package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.MetricInterceptor.OkHttpMetricsInterceptor
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.measureNanoTime

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private const val THREAD_SLEEP_MILLIS = 5L
        private const val PROCESSING_TIME_MILLIS = 6000
        private const val MAX_RETRY_COUNT = 3
        private val RETRYABLE_HTTP_CODES = setOf(429, 500, 502, 503, 504)
        private const val DELAY_DURATION_MILLIS = 25L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val MAX_PAYMENT_REQUEST_DURATION = properties.averageProcessingTime.toMillis() * 2
    private val v = AtomicInteger(0)
    val responseTimes = ConcurrentLinkedQueue<Long>()

    private val client = OkHttpClient.Builder()
        .addInterceptor(OkHttpMetricsInterceptor())
        .callTimeout(Duration.ofMillis(MAX_PAYMENT_REQUEST_DURATION))
        .build();
    private val rateLimiter = TokenBucketRateLimiter(
        rate = rateLimitPerSec,
        window = 1005,
        bucketMaxCapacity = rateLimitPerSec,
        timeUnit = TimeUnit.MILLISECONDS
    )

    private val semaphore = Semaphore(parallelRequests, true)
    private val acquireMaxWaitMillis = PROCESSING_TIME_MILLIS - requestAverageProcessingTime.toMillis()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        v.getAndIncrement()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

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
        }.build()

        var isAcquired = false
        var threadWaitTime = 0L

        var curIteration = 0
        while (curIteration < MAX_RETRY_COUNT) {
            var isRetryableWithDelay = false
            try {
                while (!rateLimiter.tick()) {
                    threadWaitTime += THREAD_SLEEP_MILLIS
                    Thread.sleep(THREAD_SLEEP_MILLIS)
                }

                isAcquired = semaphore.tryAcquire(acquireMaxWaitMillis - threadWaitTime, TimeUnit.MILLISECONDS)
                if (!isAcquired) {
                    throw TimeoutException("Failed to acquire permission to process payment")
                }

                var isSuccessRequest = false
                val duration = measureNanoTime {
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

                        if (v.get() % 50 == 0) {
                            logger.info(" %%%%%%%%%%%%%%%%%%%%%% Response Time Percentiles: ${calculatePercentiles()}")
                        }

                        if (body.result) {
                            isSuccessRequest = true
                        }
                        if (RETRYABLE_HTTP_CODES.contains(response.code)) isRetryableWithDelay = true
                    }
                }

                responseTimes.add(duration / 1_000_000)
                if (isSuccessRequest) { return}

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
                if (isAcquired) {
                    semaphore.release()
                }
            }

            curIteration++
            if (curIteration < MAX_RETRY_COUNT && isRetryableWithDelay) {
                logger.warn("[$accountName]/ Retry for payment processed for txId: $transactionId, payment: $paymentId")
                Thread.sleep(DELAY_DURATION_MILLIS)
            }
            if (now() + requestAverageProcessingTime.toMillis() * 1.2 >= deadline) return
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    fun calculatePercentiles(): Map<Int, Long> {
        val sortedTimes = responseTimes.sorted()
        return mapOf(
            50 to percentile(sortedTimes, 50),
            85 to percentile(sortedTimes, 85),
            86 to percentile(sortedTimes, 86),
            87 to percentile(sortedTimes, 87),
            88 to percentile(sortedTimes, 88),
            89 to percentile(sortedTimes, 89),
            90 to percentile(sortedTimes, 80),
            91 to percentile(sortedTimes, 90),
            92 to percentile(sortedTimes, 92),
            93 to percentile(sortedTimes, 93),
            94 to percentile(sortedTimes, 94),
            95 to percentile(sortedTimes, 95),
            96 to percentile(sortedTimes, 96),
            97 to percentile(sortedTimes, 97),
            98 to percentile(sortedTimes, 98),
            99 to percentile(sortedTimes, 99)
        )
    }

    fun percentile(data: List<Long>, percentile: Int): Long {
        if (data.isEmpty()) return 0
        val index = (percentile / 100.0 * data.size).toInt().coerceAtMost(data.size - 1)
        return data[index]
    }

}

public fun now() = System.currentTimeMillis()