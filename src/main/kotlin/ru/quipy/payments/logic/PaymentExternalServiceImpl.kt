package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore


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
    private val processingTime = properties.averageProcessingTime

    private val client = OkHttpClient.Builder().build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val semaphore = Semaphore(parallelRequests)

    // Используем глобальный MeterRegistry
    private val meterRegistry: MeterRegistry = Metrics.globalRegistry

    // Метрики
    // Инициализация метрик с тегами для лучшей идентификации
    private val semaphoreRequestsCounter: Counter = Counter.builder("payment.semaphore.requests.total")
        .description("Total number of requests to the semaphore")
        .tag("account", accountName)
        .tag("service", serviceName)
        .register(meterRegistry)
    private val semaphoreAcquiredCounter: Counter = Counter.builder("payment.semaphore.acquired.total")
        .description("Total number of requests that acquired the semaphore")
        .tag("account", accountName)
        .tag("service", serviceName)
        .register(meterRegistry)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.debug("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Увеличиваем счетчик запросов к семафору
        semaphoreRequestsCounter.increment()

        try {
            semaphore.acquire()

            // Увеличиваем счетчик успешных захватов семафора
            semaphoreAcquiredCounter.increment()

            rateLimiter.tickWithPriorityBlocking(deadline)

            val request = Request.Builder()
                .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                .post(emptyBody)
                .build()

            client.newCall(request).execute().use { response ->
                var body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                while (!body.result) {
                    client.newCall(request).execute().use { response ->
                        logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message} with code ${response.code}\nWaiting 200ms")
                        Thread.sleep(Duration.ofMillis(200))
                        body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }
                    }
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message} with code ${response.code}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } finally {
            semaphore.release()
        }
    }

    override fun approximateWaitingTime(queueLength: Long): Long {
        return queueLength / rateLimitPerSec * 1000 + processingTime.toMillis()
    }

    override fun getRateLimit(): Long {
        return rateLimitPerSec.toLong()
    }

    override fun getProcessingTime(): Duration {
       return processingTime
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()