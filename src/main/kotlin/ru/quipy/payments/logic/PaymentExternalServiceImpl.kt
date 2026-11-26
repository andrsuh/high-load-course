package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.Call
import okhttp3.Callback
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private const val ESTIMATE_SAFETY_FACTOR = 1.1

        private const val MIN_LATENCY_SAMPLES = 50
        private const val COLD_START_LATENCY_FACTOR = 1.2
        private const val LATENCY_ESTIMATE_REFRESH_MS = 200
        private const val RATE_WINDOW_MARGIN_MS = 20L
        private const val MAX_RETRIES = 10
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateWindow = Duration.ofMillis(1_000 + RATE_WINDOW_MARGIN_MS)
    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        rateWindow)

    private val ongoingWindow = OngoingWindow(parallelRequests)
    private val rateWaiters = ReentrantLock(true)
    private val retryExecutor = ThreadPoolExecutor(
        1,
        1,
        0L,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue<Runnable>(parallelRequests),
        NamedThreadFactory("payment-retry-$accountName"),
        ThreadPoolExecutor.AbortPolicy(),
    )

    private val effectiveParallel = maxOf(
        1,
        minOf(parallelRequests.toLong(), rateLimitPerSec.toLong() * requestAverageProcessingTime.toMillis() / 1000).toInt()
    )

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .connectionPool(ConnectionPool(effectiveParallel * 2, 5, TimeUnit.MINUTES))
        .build()

    private val inSystem = AtomicInteger()
    private val rateGranted = AtomicInteger()

    private val providerLatency = Timer.builder("payment.provider.duration")
        .tag("account", accountName)
        .publishPercentiles(0.95)
        .distributionStatisticExpiry(Duration.ofSeconds(30))
        .register(meterRegistry)

    @Volatile
    private var processingEstimateMs = (requestAverageProcessingTime.toMillis() * COLD_START_LATENCY_FACTOR).toLong()

    @Volatile
    private var processingEstimateUpdatedAt = 0L

    private fun processingEstimateMs(): Long {
        val now = now()
        if (now - processingEstimateUpdatedAt > LATENCY_ESTIMATE_REFRESH_MS) {
            processingEstimateUpdatedAt = now
            val p95 = providerLatency.takeSnapshot().percentileValues().firstOrNull()?.value(TimeUnit.MILLISECONDS)
            if (providerLatency.count() >= MIN_LATENCY_SAMPLES && p95 != null && p95 > 0) {
                processingEstimateMs = maxOf(requestAverageProcessingTime.toMillis(), p95.toLong())
            }
        }
        return processingEstimateMs
    }

    override fun tryReserve(deadline: Long): Boolean {
        val position = inSystem.incrementAndGet()
        val processingMs = processingEstimateMs().coerceAtLeast(1)
        val throughputPerMs = minOf(
            rateLimitPerSec.toDouble() / rateWindow.toMillis(),
            parallelRequests.toDouble() / processingMs
        )
        val queueWaitMs = maxOf(
            maxOf(0, position - effectiveParallel) / throughputPerMs,
            rateLimiter.estimatedDelayMillis(maxOf(1, position - rateGranted.get())).toDouble()
        )
        val expectedCompletion = now() + (queueWaitMs + processingMs) * ESTIMATE_SAFETY_FACTOR
        if (expectedCompletion > deadline) {
            inSystem.decrementAndGet()
            return false
        }
        return true
    }

    override fun releaseReservation() {
        inSystem.decrementAndGet()
    }

    private fun waitBudget(deadline: Long) = deadline - now() - processingEstimateMs()

    private fun awaitRate(deadline: Long): Boolean {
        val budget = waitBudget(deadline)
        if (budget <= 0 || !rateWaiters.tryLock(budget, TimeUnit.MILLISECONDS)) return false
        return try {
            rateLimiter.tickBlocking(waitBudget(deadline))
        } finally {
            rateWaiters.unlock()
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        performPaymentAttempt(paymentId, amount, paymentStartedAt, deadline, UUID.randomUUID(), MAX_RETRIES)
    }

    private fun performPaymentAttempt(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        transactionId: UUID,
        retriesRemaining: Int,
    ) {
        var acquired = false
        var granted = false
        var handedOff = false

        try {
            acquired = ongoingWindow.tryAcquire(waitBudget(deadline))
            if (!acquired) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Payment deadline exceeded.")
                }
                return
            }

            logger.warn("[$accountName] Submitting payment request for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            if (!awaitRate(deadline)) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Payment deadline exceeded.")
                }
                return
            }
            rateGranted.incrementAndGet()
            granted = true

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val call = client.newCall(request)
            call.timeout().timeout(maxOf(1, deadline - now()), TimeUnit.MILLISECONDS)
            val sentAt = now()

            call.enqueue(object : Callback {
                override fun onResponse(call: Call, response: Response) {
                    providerLatency.record(now() - sentAt, TimeUnit.MILLISECONDS)
                    var attemptReleased = false
                    var retryScheduled = false
                    try {
                        val body = response.use { readResponse(it, transactionId, paymentId) }
                        if (!body.result && response.isSuccessful && retriesRemaining > 0 && waitBudget(deadline) > 0) {
                            releaseAttempt()
                            attemptReleased = true
                            retryExecutor.execute {
                                performPaymentAttempt(paymentId, amount, paymentStartedAt, deadline, transactionId, retriesRemaining - 1)
                            }
                            retryScheduled = true
                        } else {
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                        }
                    } catch (e: Exception) {
                        handleFailure(e, transactionId, paymentId)
                    } finally {
                        if (!attemptReleased) releaseAttempt()
                        if (!retryScheduled) releaseReservation()
                    }
                }

                override fun onFailure(call: Call, e: IOException) {
                    providerLatency.record(now() - sentAt, TimeUnit.MILLISECONDS)
                    try {
                        handleFailure(e, transactionId, paymentId)
                    } finally {
                        complete()
                    }
                }
            })
            handedOff = true
        } catch (e: Exception) {
            handleFailure(e, transactionId, paymentId)
        } finally {
            if (!handedOff) {
                if (granted) rateGranted.decrementAndGet()
                if (acquired) {
                    ongoingWindow.release()
                }
                releaseReservation()
            }
        }
    }

    override fun close() {
        retryExecutor.shutdown()
        client.dispatcher.cancelAll()
        client.dispatcher.executorService.shutdown()
        client.connectionPool.evictAll()
    }

    private fun complete() {
        releaseAttempt()
        releaseReservation()
    }

    private fun releaseAttempt() {
        rateGranted.decrementAndGet()
        ongoingWindow.release()
    }

    private fun readResponse(response: Response, transactionId: UUID, paymentId: UUID): ExternalSysResponse {
        val responseBody = response.body?.string()
        val body = try {
            mapper.readValue(responseBody, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $responseBody")
            throw e
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

        return body
    }

    private fun handleFailure(e: Exception, transactionId: UUID, paymentId: UUID) {
        when (e) {
            is InterruptedIOException -> {
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

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
