package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
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
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

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
    private val summary =  DistributionSummary
        .builder("request_latency_seconds")
        .tags("service", "payment")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val retryCounter = Counter
        .builder("payment_retry_count")
        .tag("account", accountName)
        .register(registry)

    private val inflightGauge = Gauge
        .builder("inflight_requests", this) { adapter ->
            adapter.currentInflight.get().toDouble()
        }
        .tag("account", accountName)
        .register(registry)

    private val currentInflight = AtomicInteger(0)

    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    private val semaphoreToLimitParallelRequest = OngoingWindow(parallelRequests)
    private val slidingWindowRateLimiter =
        SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val lastDurations = LinkedBlockingDeque<Long>(1000)

    private fun quantile(q: Double): Long {
        val copy = lastDurations.toList()
        if (copy.isEmpty()) return 2000L
        val sorted = copy.sorted()
        val indexes = ((sorted.size - 1) * q).toInt().coerceIn(0, sorted.size - 1)
        return sorted[indexes]
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        try {
            semaphoreToLimitParallelRequest.acquire()
            currentInflight.incrementAndGet()
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
                // Максимум три попытки, чтобы не пытаться бесконечно решить не работающий запрос
                while (attempt <= 3 && !success) {
                    val startTime = now()
                    val remaining = deadline - now()
                    if (remaining <= 200) {
                        break
                    }

                    val histP90 = quantile(0.90)
                    val attemptTimeout = histP90.coerceAtLeast(500L).coerceAtMost(10_000L).coerceAtMost(remaining - 50)
                    if (attemptTimeout <= 0L) {
                        break
                    }

                    if (attempt > 1) {
                        retryCounter.increment()
                        logger.info("[$accountName] Retry #$attempt for payment $paymentId")
                    }

                    try {
                        val startTime = now()
                        val clientWithTimeout = client.newBuilder()
                            .callTimeout(Duration.ofMillis(attemptTimeout))
                            .readTimeout(Duration.ofMillis(attemptTimeout))
                            .build()
                        val request = Request.Builder().run {
                            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                            post(emptyBody)
                        }.build()

                        clientWithTimeout.newCall(request).execute().use { response ->
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

                            val durationSeconds = (now() - startTime).toDouble() / 1000.0
                            summary.record(durationSeconds)

                            if (body.result) {
                                success = true
                            }
                        }
                    } catch (e: Exception) {
                        if (e is SocketTimeoutException || e.cause is SocketTimeoutException) {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), null, reason = "Request timeout")
                            }
                        }

                        else {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), null, reason = e.message ?: "unknown")
                            }
                        }
                    } finally {
                        val duration = now() - startTime
                        if (lastDurations.size >= 1000) lastDurations.pollFirst()
                        lastDurations.offerLast(duration)
                    }

                    if (!success) {
                        val adder = ThreadLocalRandom.current().nextLong(0, 100)
                        val backoff = (200L * (1L shl (attempt - 1))).coerceAtMost(2000L)
                        val beforeDeadline = deadline - now()
                        if (beforeDeadline <= 100)  {
                            break
                        }
                        val actualSleep = minOf(backoff + adder, beforeDeadline - 50)
                        if (actualSleep > 0) {
                            try {
                                Thread.sleep(actualSleep)
                            } catch (e: InterruptedException) {
                                Thread.currentThread().interrupt();
                                break
                            }
                        }
                    }
                    attempt++
                }
            } finally {
                currentInflight.decrementAndGet()
                semaphoreToLimitParallelRequest.release()
            }
        } catch (e: InterruptedException) {
            logger.error("Problem while performing payment", e)
            Thread.currentThread().interrupt()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()