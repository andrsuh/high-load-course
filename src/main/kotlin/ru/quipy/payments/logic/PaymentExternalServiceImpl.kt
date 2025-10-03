package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
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

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val windowControl = OngoingWindow(parallelRequests);


    private val externalTimer: Timer = Timer.builder("payments_external_request_seconds")
        .description("HTTP call duration to external payment provider")
        .tags("account", accountName)
        .publishPercentileHistogram()
        .register(meterRegistry)

    private val rateWaitTimer: Timer = Timer.builder("payments_rate_wait_ms")
        .description("Time spent waiting for rate limiter")
        .tags("account", accountName)
        .register(meterRegistry)

    private val parallelWaitTimer: Timer = Timer.builder("payments_parallel_wait_ms")
        .description("Time spent waiting for parallel window")
        .tags("account", accountName)
        .register(meterRegistry)

    private val successCounter: Counter = Counter.builder("payments_external_success_total")
        .description("Successful external payments")
        .tags("account", accountName)
        .register(meterRegistry)

    private val errorCounter: Counter = Counter.builder("payments_external_errors_total")
        .description("Failed external payments")
        .tags("account", accountName)
        .register(meterRegistry)

    private val inflight = AtomicInteger(0)

    init {
        Gauge.builder("payments_window_queue_size", windowControl) { it.awaitingQueueSize().toDouble() }
            .description("Threads waiting on window semaphore")
            .tags("account", accountName)
            .strongReference(true)
            .register(meterRegistry)

        Gauge.builder("payments_inflight", inflight) { it.get().toDouble() }
            .description("Current in-flight external requests (account window)")
            .tags("account", accountName)
            .strongReference(true)
            .register(meterRegistry)
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

        var windowAcquired = false
        var rateWaitNanos = 0L
        var parallelWaitNanos = 0L

        try {
            while (true) {
                val pwStart = System.nanoTime()

                windowControl.acquire()
                parallelWaitNanos += (System.nanoTime() - pwStart)

                windowAcquired = true
                inflight.incrementAndGet()

                if (rateLimiter.tick()) {
                    break
                } else {
                    windowControl.release()
                    windowAcquired = false

                    inflight.decrementAndGet()

                    val rwStart = System.nanoTime()
                    rateLimiter.tickBlocking()
                    rateWaitNanos += (System.nanoTime() - rwStart)
                }
            }

            if (rateWaitNanos > 0) rateWaitTimer.record(rateWaitNanos, TimeUnit.NANOSECONDS)
            if (parallelWaitNanos > 0) parallelWaitTimer.record(parallelWaitNanos, TimeUnit.NANOSECONDS)

            val sample = Timer.start(meterRegistry)

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            client.newCall(request).execute().use { response ->
                val raw = response.body?.string() ?: ""
                val body = runCatching { mapper.readValue(raw, ExternalSysResponse::class.java) }
                    .getOrElse { e ->
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, code: ${response.code}, body: $raw", e)
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                sample.stop(externalTimer)

                if (body.result) successCounter.increment() else errorCounter.increment()

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
                    errorCounter.increment()
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    errorCounter.increment()
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            if (windowAcquired) {
                windowControl.release()
                inflight.decrementAndGet()
            }
        }
    }



    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()