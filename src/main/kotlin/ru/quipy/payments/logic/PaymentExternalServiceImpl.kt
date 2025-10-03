package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
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


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val semaphore = Semaphore(parallelRequests)

    // Используем глобальный MeterRegistry
    private val meterRegistry: MeterRegistry = Metrics.globalRegistry

    // Метрики
    private val semaphoreRequestsCounter: Counter
    private val semaphoreAcquiredCounter: Counter
    private val semaphoreTimeoutCounter: Counter
    private val semaphoreWaitTimer: Timer

    init {
        // Инициализация метрик с тегами для лучшей идентификации
        semaphoreRequestsCounter = Counter.builder("payment.semaphore.requests.total")
            .description("Total number of requests to the semaphore")
            .tag("account", accountName)
            .tag("service", serviceName)
            .register(meterRegistry)

        semaphoreAcquiredCounter = Counter.builder("payment.semaphore.acquired.total")
            .description("Total number of requests that acquired the semaphore")
            .tag("account", accountName)
            .tag("service", serviceName)
            .register(meterRegistry)

        semaphoreTimeoutCounter = Counter.builder("payment.semaphore.timeouts.total")
            .description("Total number of requests that timed out waiting for semaphore")
            .tag("account", accountName)
            .tag("service", serviceName)
            .register(meterRegistry)

        semaphoreWaitTimer = Timer.builder("payment.semaphore.wait.duration")
            .description("Time spent waiting for semaphore acquisition")
            .tag("account", accountName)
            .tag("service", serviceName)
            .register(meterRegistry)

        // Метрика для текущего состояния семафора (доступные разрешения)
        meterRegistry.gauge("payment.semaphore.available.permits",
            listOf(
                io.micrometer.core.instrument.Tag.of("account", accountName),
                io.micrometer.core.instrument.Tag.of("service", serviceName)
            ),
            semaphore
        ) { obj -> obj.availablePermits().toDouble() }

        // Метрика для максимального количества разрешений
        meterRegistry.gauge("payment.semaphore.max.permits",
            listOf(
                io.micrometer.core.instrument.Tag.of("account", accountName),
                io.micrometer.core.instrument.Tag.of("service", serviceName)
            ),
            semaphore
        ) { obj -> parallelRequests.toDouble() }

        // Метрика для длины очереди (сколько потоков ждут)
        meterRegistry.gauge("payment.semaphore.queue.length",
            listOf(
                io.micrometer.core.instrument.Tag.of("account", accountName),
                io.micrometer.core.instrument.Tag.of("service", serviceName)
            ),
            semaphore
        ) { obj -> obj.queueLength.toDouble() }

        logger.info("Metrics initialized for account: $accountName, service: $serviceName")
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        val remainingTime = (deadline - now()) / 2

        // Увеличиваем счетчик запросов к семафору
        semaphoreRequestsCounter.increment()

        try {
            if (!semaphore.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)) {
                logger.warn("[$accountName] Rejecting payment $paymentId: parallel requests limit reached")
                // Увеличиваем счетчик таймаутов
                semaphoreTimeoutCounter.increment()
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Parallel requests limit reached")
                }
                return
            }

            // Увеличиваем счетчик успешных захватов семафора
            semaphoreAcquiredCounter.increment()

            rateLimiter.tickBlocking()

            try {
                val request = Request.Builder()
                    .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    .post(emptyBody)
                    .build()

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            } finally {
                semaphore.release()
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

}

fun now() = System.currentTimeMillis()