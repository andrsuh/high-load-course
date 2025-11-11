package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


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

        private fun now() = System.currentTimeMillis()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // SlidingWindowRateLimiter - точный контроль RPS в скользящем окне
    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    // Bulkhead - автоматическое управление параллельностью и очередью
    private val bulkhead = Bulkhead.of(
        "payment-bulkhead-$accountName",
        BulkheadConfig.custom()
            .maxConcurrentCalls(parallelRequests)
            .maxWaitDuration(Duration.ofSeconds(30))
            .build()
    )

    private val client = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(parallelRequests * 2, 5, TimeUnit.MINUTES))
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(10, TimeUnit.SECONDS)
        .build()

    // HashMap для отслеживания задержек retry для каждого payment
    private val retryDelayMap = HashMap<UUID, Long>()

    init {
        logger.info("[$accountName] Initialized with parallelRequests=$parallelRequests, rate limit=$rateLimitPerSec rps")
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, activeRequestsCount: AtomicInteger) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Counter: увеличиваем счетчик отправленных запросов
        metrics.incrementSubmissions(accountName)

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        // Retry логика: 3 попытки
        // 1-я попытка: сразу (delay = 0)
        // 2-я попытка: после delay = averageProcessingTime
        // 3-я попытка: после delay = averageProcessingTime * 2
        for (attempt in 1..3) {
            if (now() >= deadline) {
                metrics.incrementTimeout(accountName, "deadline_expired_before_attempt_$attempt")
                metrics.incrementFailure(accountName, "deadline_expired")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline expired before attempt $attempt")
                }
                break
            }

            val success = performBankPayment(transactionId, paymentId, amount, paymentStartedAt, deadline, activeRequestsCount, attempt)

            if (success) {
                // Успех - убираем из map и выходим
                retryDelayMap.remove(paymentId)
                return
            }

            // Не успех - вычисляем задержку для следующей попытки
            if (attempt < 3) {
                val currentDelay = retryDelayMap[paymentId] ?: 0L
                Thread.sleep(currentDelay)

                // Увеличиваем задержку для следующей попытки
                retryDelayMap[paymentId] = currentDelay + requestAverageProcessingTime.toMillis()

                logger.info("[$accountName] Payment $paymentId attempt $attempt failed, will retry after ${currentDelay}ms")
            }
        }

        // Все 3 попытки провалились - убираем из map
        retryDelayMap.remove(paymentId)
    }

    private fun performBankPayment(
        transactionId: UUID,
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        activeRequestsCount: AtomicInteger,
        attempt: Int
    ): Boolean {
        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            return sendRequest(request, paymentId, transactionId, paymentStartedAt, deadline, activeRequestsCount, attempt)
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    metrics.incrementTimeout(accountName, "socket_timeout_attempt_$attempt")
                    metrics.incrementFailure(accountName, "request_timeout")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout on attempt $attempt")
                    }
                }
                else -> {
                    metrics.incrementFailure(accountName, e.javaClass.simpleName)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
            return false
        }
    }

    private fun sendRequest(
        request: Request,
        paymentId: UUID,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        activeRequestsCount: AtomicInteger,
        attempt: Int
    ): Boolean {
        var result = false

        // Bulkhead управляет параллельностью и очередью
        bulkhead.executeCallable {
            // SlidingWindowRateLimiter блокирует до освобождения слота RPS
            rateLimiter.tickBlocking()

            activeRequestsCount.incrementAndGet()
            try {
                // Проверка deadline
                if (now() >= deadline) {
                    metrics.incrementTimeout(accountName, "deadline_expired_in_request")
                    metrics.incrementFailure(accountName, "deadline_expired")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline expired during request")
                    }
                    return@executeCallable
                }

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, attempt: $attempt, succeeded: ${body.result}, message: ${body.message}")

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

                    result = body.result
                }
            } finally {
                activeRequestsCount.decrementAndGet()
            }
        }

        return result
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun getOptimalThreads(): Int = parallelRequests

    override fun getQueueSize(): Int = 0 // Bulkhead управляет очередью

}
