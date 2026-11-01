package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
    private val expectedProcessingTimeMs: Long = 0L,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private const val RATE_WAIT_MAX_MS = 500L

        /** Шаг опроса лимитера. */
        private const val RATE_WAIT_STEP_MS = 5L

        /** Минимальный бюджет ожидания слота: лучше отправить запрос с малым запасом, чем гарантированно провалить его. */
        private const val MIN_WAIT_BUDGET_MS = 50L

        /** Вес предыдущей оценки в экспоненциальном сглаживании. */
        private const val EWMA_PREVIOUS_WEIGHT = 4L

        /**
         * Пределы диспетчера OkHttp. Подняты выше дефолтных (64 / 5), чтобы обеспечить
         * in-flight, необходимый для полной утилизации: 11 rps × 13 с = 143 запроса.
         */
        private const val DISPATCHER_MAX_REQUESTS = 512
        private const val DISPATCHER_MAX_REQUESTS_PER_HOST = 256
        private const val EGRESS_RATE_HEADROOM = 1
        private const val EGRESS_BUCKET_SIZE = 1
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(2))
        .writeTimeout(Duration.ofSeconds(10))
        .readTimeout(Duration.ofSeconds(60))
        .callTimeout(Duration.ofSeconds(90))
        .dispatcher(
            Dispatcher().apply {
                maxRequests = DISPATCHER_MAX_REQUESTS
                maxRequestsPerHost = DISPATCHER_MAX_REQUESTS_PER_HOST
            }
        )
        .connectionPool(ConnectionPool(DISPATCHER_MAX_REQUESTS_PER_HOST, 5L, TimeUnit.MINUTES))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    private val parallelLimiter = Semaphore(parallelRequests, true)

    private val deadlineDrops: Counter = meterRegistry.counter("payment_dropped_total", "reason", "deadline")
    private val slotDrops: Counter = meterRegistry.counter("payment_dropped_total", "reason", "slot")
    private val rateWaitDrops: Counter = meterRegistry.counter("payment_dropped_total", "reason", "rate_wait")

    private val observedProcessingMs = AtomicLong(initialProcessingEstimate())

    private fun initialProcessingEstimate(): Long {
        if (expectedProcessingTimeMs > 0) return expectedProcessingTimeMs
        return properties.averageProcessingTime.toMillis()
    }

    private fun observeProcessingMs(durationMs: Long) {
        if (durationMs <= 0) return
        while (true) {
            val previous = observedProcessingMs.get()
            val next = if (previous <= 0) {
                durationMs
            } else {
                (previous * EWMA_PREVIOUS_WEIGHT + durationMs) / (EWMA_PREVIOUS_WEIGHT + 1)
            }
            if (observedProcessingMs.compareAndSet(previous, next)) return
        }
    }

    private fun awaitRatePermission(budgetMs: Long): Boolean {
        val waitDeadline = now() + minOf(budgetMs.coerceAtLeast(0L), RATE_WAIT_MAX_MS)
        while (true) {
            if (rateLimiter.tick()) return true
            if (now() >= waitDeadline) return false
            Thread.sleep(RATE_WAIT_STEP_MS)
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val budgetMs = deadline - now()
        if (budgetMs <= 0) {
            dropPayment(
                paymentId,
                transactionId,
                deadlineDrops,
                "Deadline exceeded before submission, request not sent"
            )
            return
        }

        val waitBudgetMs = maxOf(MIN_WAIT_BUDGET_MS, budgetMs - observedProcessingMs.get())
        if (!parallelLimiter.tryAcquire(waitBudgetMs, TimeUnit.MILLISECONDS)) {
            dropPayment(paymentId, transactionId, slotDrops, "Deadline would be missed, request not sent")
            return
        }

        try {
            if (!awaitRatePermission(budgetMs)) {
                dropPayment(paymentId, transactionId, rateWaitDrops, "Rate limit wait exceeded, request not sent")
                return
            }

            val sentAt = now()

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            client.newCall(request).execute().use { response ->
                observeProcessingMs(now() - sentAt)

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
            parallelLimiter.release()
        }
    }

    private fun dropPayment(paymentId: UUID, transactionId: UUID, counter: Counter, reason: String) {
        counter.increment()
        logger.error("[$accountName] Payment $paymentId dropped: $reason, txId: $transactionId")
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()