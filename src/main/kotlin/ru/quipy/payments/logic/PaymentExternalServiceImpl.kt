package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.Semaphore;
import okhttp3.*
import okhttp3.Protocol
import java.io.IOException
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
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
    private val virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor()

    private val client = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(
            maxIdleConnections = parallelRequests,
            keepAliveDuration = 5,
            timeUnit = TimeUnit.MINUTES
        ))
        .dispatcher(Dispatcher(virtualThreadExecutor).apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .connectTimeout(Duration.ofSeconds(5))
        .writeTimeout(Duration.ofSeconds(10))
        .build()
    private val semaphoreToLimitParallelRequest = OngoingWindow(parallelRequests)
    private val slidingWindowRateLimiter =
        SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val lastDurations = LinkedBlockingDeque<Long>(1000)

    private fun quantile(q: Double): Long {
        val copy = lastDurations.toList()
        if (copy.isEmpty()) return 50000L
        val sorted = copy.sorted()
        val indexes = ((sorted.size - 1) * q).toInt().coerceIn(0, sorted.size - 1)
        return sorted[indexes]
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Void> {
        val resultFuture = CompletableFuture<Void>()

        try {
            val timeUntilDeadline = deadline - now()
            if (timeUntilDeadline < 5000) {
                logger.warn("[$accountName] Payment $paymentId rejected - deadline too close (${timeUntilDeadline}ms)")
                resultFuture.complete(null)
                return resultFuture
            }

            logger.warn("[$accountName] Submitting payment request for payment $paymentId (deadline in ${timeUntilDeadline}ms, inflight: ${currentInflight.get()})")
            slidingWindowRateLimiter.tickBlocking()

            semaphoreToLimitParallelRequest.acquire()
            currentInflight.incrementAndGet()

            val transactionId = UUID.randomUUID()

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

                logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            performRequestWithRetryAsync(paymentId, amount, transactionId, deadline, 1)
                .whenComplete { _, throwable ->
                    currentInflight.decrementAndGet()
                    semaphoreToLimitParallelRequest.release()

                    if (throwable != null) {
                        logger.error("[$accountName] Async payment processing failed for payment $paymentId", throwable)
                        resultFuture.completeExceptionally(throwable)
                    } else {
                        resultFuture.complete(null)
                    }
                }

        } catch (e: Exception) {
            currentInflight.decrementAndGet()
            semaphoreToLimitParallelRequest.release()
            logger.error("[$accountName] Error initiating payment $paymentId", e)
            resultFuture.completeExceptionally(e)
        }

        return resultFuture
    }

    private fun performRequestWithRetryAsync(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
        attempt: Int
    ): CompletableFuture<Void> {
        val future = CompletableFuture<Void>()

        val remaining = deadline - now()
        if (remaining <= 200 || attempt > 3) {
            future.complete(null)
            return future
        }

        val histP95 = quantile(0.95)
        // Для кейса 9: среднее 10s, max 50s
        // При 1000 RPS * 30s = 30,000 in-flight, но semaphore только 20,000
        // Поэтому timeout должен быть еще короче для первых попыток
        val attemptTimeout = if (attempt == 1) {
            histP95.coerceAtLeast(15000L).coerceAtMost(25_000L).coerceAtMost(remaining - 100)
        } else {
            histP95.coerceAtLeast(20000L).coerceAtMost(40_000L).coerceAtMost(remaining - 100)
        }
        if (attemptTimeout <= 0L) {
            future.complete(null)
            return future
        }

        if (attempt > 1) {
            retryCounter.increment()
            logger.info("[$accountName] Retry #$attempt for payment $paymentId")
        }

        val requestStartTime = now()
        val attemptStartTime = now()


        val clientWithTimeout = client.newBuilder()
            .callTimeout(Duration.ofMillis(attemptTimeout))
            .readTimeout(Duration.ofMillis(attemptTimeout))
            .build()

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        clientWithTimeout.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                val duration = now() - attemptStartTime
                if (lastDurations.size >= 1000) lastDurations.pollFirst()
                lastDurations.offerLast(duration)

                if (e is SocketTimeoutException || e.cause is SocketTimeoutException) {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), null, reason = "Request timeout")
                    }
                } else {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), null, reason = e.message ?: "unknown")
                    }
                }

                // Retry if possible
                if (attempt < 3 && deadline - now() > 60) {
                    scheduleRetry(paymentId, amount, transactionId, deadline, attempt, future)
                } else {
                    future.complete(null)
                }
            }

            override fun onResponse(call: Call, response: Response) {
                response.use {
                    val duration = now() - attemptStartTime
                    if (lastDurations.size >= 1000) lastDurations.pollFirst()
                    lastDurations.offerLast(duration)

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

                    summary.record((now() - requestStartTime) / 1000.0)

                    if (body.result) {
                        future.complete(null)
                    } else if (attempt < 3 && deadline - now() > 60) {
                        scheduleRetry(paymentId, amount, transactionId, deadline, attempt, future)
                    } else {
                        future.complete(null)
                    }
                }
            }
        })

        return future
    }

    private fun scheduleRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
        attempt: Int,
        parentFuture: CompletableFuture<Void>
    ) {
        val adder = ThreadLocalRandom.current().nextLong(0, 100)
        val backoff = (200L * (1L shl (attempt - 1))).coerceAtMost(2000L)
        val beforeDeadline = deadline - now()

        if (beforeDeadline <= 60) {
            parentFuture.complete(null)
            return
        }

        val actualSleep = minOf(backoff + adder, beforeDeadline - 60)
        if (actualSleep <= 0) {
            parentFuture.complete(null)
            return
        }

        CompletableFuture.delayedExecutor(actualSleep, TimeUnit.MILLISECONDS).execute {
            performRequestWithRetryAsync(paymentId, amount, transactionId, deadline, attempt + 1)
                .whenComplete { _, throwable ->
                    if (throwable != null) {
                        parentFuture.completeExceptionally(throwable)
                    } else {
                        parentFuture.complete(null)
                    }
                }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()