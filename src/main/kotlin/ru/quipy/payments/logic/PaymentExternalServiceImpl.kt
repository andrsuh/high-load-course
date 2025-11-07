package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.ConnectionPool
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.config.PaymentMetrics
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import ru.quipy.common.utils.NamedThreadFactory


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metrics: PaymentMetrics,
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

    private val optimalThreads = maxOf(
        (rateLimitPerSec * requestAverageProcessingTime.seconds * 3).toInt(),
        parallelRequests
    )

    private val client = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(optimalThreads * 2, 5, TimeUnit.MINUTES))
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(10, TimeUnit.SECONDS)
        .build()

    private val parallelRequestSemaphore = Semaphore(optimalThreads)

    private val asyncExecutor = ThreadPoolExecutor(
        optimalThreads,
        optimalThreads,
        60L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(500),
        NamedThreadFactory("payment-async-$accountName")
    )

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    init {
        // Регистрируем Gauge метрики для мониторинга состояния очередей и потоков
        metrics.registerQueueSizeGauge(accountName) { asyncExecutor.queue.size }
        metrics.registerActiveThreadsGauge(accountName) { asyncExecutor.activeCount }
        metrics.registerSemaphoreAvailableGauge(accountName) { parallelRequestSemaphore.availablePermits() }

        logger.info("[$accountName] Initialized with $optimalThreads threads, rate limit $rateLimitPerSec rps")
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, activeRequestsCount: java.util.concurrent.atomic.AtomicInteger) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Counter: увеличиваем счетчик отправленных запросов
        metrics.incrementSubmissions(accountName)

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        activeRequestsCount.incrementAndGet()

        asyncExecutor.submit {
            try {
                executeWithSemaphore(paymentId, transactionId, amount, deadline, paymentStartedAt)
            } finally {
                activeRequestsCount.decrementAndGet()
            }
        }
    }

    private fun executeWithSemaphore(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long, paymentStartedAt: Long) {
        val queueEnterTime = now()
        try {
            val now = now()

            if (now >= deadline) {
                metrics.incrementTimeout(accountName, "deadline_before_semaphore")
                metrics.incrementFailure(accountName, "deadline_expired_before_semaphore")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline expired before semaphore")
                }
                return
            }

            val timeoutMs = deadline - now
            val acquired = parallelRequestSemaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)

            if (!acquired) {
                metrics.incrementTimeout(accountName, "semaphore_timeout")
                metrics.incrementFailure(accountName, "semaphore_timeout")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Semaphore timeout - deadline approaching")
                }
                return
            }

            // Записываем время ожидания в очереди
            val queueWaitTime = now() - queueEnterTime
            metrics.recordQueueWaitTime(accountName, queueWaitTime)

            if (now() >= deadline) {
                metrics.incrementTimeout(accountName, "deadline_during_semaphore_wait")
                metrics.incrementFailure(accountName, "deadline_expired_during_semaphore_wait")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline expired during semaphore wait")
                }
                return
            }

            while (!rateLimiter.tick()) {
                if (now() >= deadline) {
                    metrics.incrementTimeout(accountName, "deadline_while_rate_limiting")
                    metrics.incrementFailure(accountName, "deadline_expired_while_rate_limiting")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline expired while waiting for rate limit")
                    }
                    return
                }

                Thread.sleep(5)
            }

            executePaymentRequest(paymentId, transactionId, amount, deadline, paymentStartedAt)
        } catch (e: InterruptedException) {
            metrics.incrementFailure(accountName, "interrupted")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Interrupted waiting for parallel slot")
            }
        } finally {
            parallelRequestSemaphore.release()
        }
    }

    private fun executePaymentRequest(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long, paymentStartedAt: Long, attempt: Int = 1) {
        val requestStartTime = now()
        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            client.newCall(request).execute().use { response ->
                if (response.code == 429 && attempt <= 10) {
                    // Метрика: повторная попытка из-за 429
                    metrics.incrementRetry(accountName, 429)

                    if (now() >= deadline) {
                        metrics.incrementTimeout(accountName, "429_retry_deadline")
                        metrics.incrementFailure(accountName, "429_retry_aborted_deadline_expired")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "429 retry aborted - deadline expired")
                        }
                        return
                    }

                    Thread.sleep(20)
                    executePaymentRequest(paymentId, transactionId, amount, deadline, paymentStartedAt, attempt + 1)
                    return
                }

                if (!response.isSuccessful && attempt <= 3) {
                    // Метрика: повторная попытка из-за ошибки
                    metrics.incrementRetry(accountName, response.code)

                    if (now() >= deadline) {
                        metrics.incrementTimeout(accountName, "retry_deadline")
                        metrics.incrementFailure(accountName, "retry_aborted_deadline_expired")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Retry aborted - deadline expired")
                        }
                        return
                    }

                    Thread.sleep(50)
                    executePaymentRequest(paymentId, transactionId, amount, deadline, paymentStartedAt, attempt + 1)
                    return
                }

                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Записываем метрики успеха/провала и время выполнения
                val totalDuration = now() - paymentStartedAt
                metrics.recordRequestDuration(accountName, totalDuration)

                if (body.result) {
                    metrics.incrementSuccess(accountName)
                } else {
                    metrics.incrementFailure(accountName, body.message ?: "unknown")
                }

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    if (attempt <= 3 && now() < deadline) {
                        // Метрика: повторная попытка из-за таймаута
                        metrics.incrementRetry(accountName, 0) // 0 = timeout

                        val delayMs = minOf(25 + (attempt * 10), 80).toLong()
                        Thread.sleep(delayMs)
                        executePaymentRequest(paymentId, transactionId, amount, deadline, paymentStartedAt, attempt + 1)
                        return
                    }

                    metrics.incrementTimeout(accountName, "socket_timeout")
                    metrics.incrementFailure(accountName, "request_timeout_after_retries")
                    val totalDuration = now() - paymentStartedAt
                    metrics.recordRequestDuration(accountName, totalDuration)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout after retries.")
                    }
                }

                else -> {
                    metrics.incrementFailure(accountName, e.javaClass.simpleName)
                    val totalDuration = now() - paymentStartedAt
                    metrics.recordRequestDuration(accountName, totalDuration)

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
