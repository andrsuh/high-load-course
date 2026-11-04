package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
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

        /** Шаг опроса лимитера. */
        private const val RATE_WAIT_STEP_MS = 5L

        /**
         * Резерв бюджета, который нельзя отдавать под ожидание окна: запись результата
         * в event store и доставка ответа клиенту после ответа платёжной системы.
         */
        private const val SUBMISSION_RESERVE_MS = 300L

        /**
         * Страховочный потолок ожидания окна — защита от некорректного дедлайна от клиента.
         * Рабочее ограничение вычисляется из дедлайна, см. [awaitRatePermission].
         */
        private const val RATE_WAIT_CEILING_MS = 30_000L

        /** Минимальный бюджет ожидания слота: лучше отправить запрос с малым запасом, чем гарантированно провалить его. */
        private const val MIN_WAIT_BUDGET_MS = 50L

        /**
         * Максимальное ожидание слота параллелизма.
         *
         * Слотов столько же, сколько параллельных запросов у аккаунта (64), а каждый вызов
         * держит слот ~1.2 с.
         */
        private const val SLOT_WAIT_MAX_MS = 5_000L

        /** Вес предыдущей оценки в экспоненциальном сглаживании. */
        private const val EWMA_PREVIOUS_WEIGHT = 4L

        /**
         * Пределы диспетчера OkHttp. Подняты, чтобы обеспечить
         * in-flight, необходимый для полной утилизации: 11 rps × 13 с = 143 запроса.
         */
        private const val DISPATCHER_MAX_REQUESTS = 512
        private const val DISPATCHER_MAX_REQUESTS_PER_HOST = 256
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    private val parallelLimiter = Semaphore(parallelRequests, true)

    private val deadlineDrops = meterRegistry.counter("payment_dropped_total", "reason", "deadline")
    private val slotDrops = meterRegistry.counter("payment_dropped_total", "reason", "slot")
    private val rateWaitDrops = meterRegistry.counter("payment_dropped_total", "reason", "rate_wait")

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

    /**
     * Ожидание разрешения выходного окна, ограниченное самим дедлайном.
     */
    private fun awaitRatePermission(budgetMs: Long): Boolean {
        val reserveMs = observedProcessingMs.get() + SUBMISSION_RESERVE_MS
        val maxWaitMs = (budgetMs - reserveMs).coerceIn(0L, RATE_WAIT_CEILING_MS)
        val waitDeadline = now() + maxWaitMs
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

        // Acquire the egress permission first. Waiting for the rate window
        // must not occupy one of the 64 provider concurrency slots.
        if (!awaitRatePermission(budgetMs)) {
            dropPayment(paymentId, transactionId, rateWaitDrops, "Rate limit wait exceeded, request not sent")
            return
        }

        // Ожидание слота ограничено константой, а не остатком дедлайна: при дедлайне 26 с
        // формула budget - observed давала до 24.5 с на поток, из-за чего пул заполнялся
        // ожидающими и клиент получал 429 от RejectedExecutionHandler.
        val waitBudgetMs = minOf(deadline - now() - observedProcessingMs.get(), SLOT_WAIT_MAX_MS)
            .coerceAtLeast(MIN_WAIT_BUDGET_MS)
        if (!parallelLimiter.tryAcquire(waitBudgetMs, TimeUnit.MILLISECONDS)) {
            dropPayment(paymentId, transactionId, slotDrops, "Deadline would be missed, request not sent")
            return
        }

        try {
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