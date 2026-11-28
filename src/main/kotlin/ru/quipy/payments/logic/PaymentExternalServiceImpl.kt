package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import okhttp3.Call
import okhttp3.Callback
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
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
import kotlin.time.Duration.Companion.milliseconds


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
        private const val LATENCY_ESTIMATE_REFRESH_MS = 100L
        private const val RATE_WINDOW_MARGIN_MS = 20L
        private const val PROVIDER_RESPONSE_GRACE_MS = 250L
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
        parallelRequests,
        parallelRequests,
        0L,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue<Runnable>(parallelRequests),
        NamedThreadFactory("payment-retry-$accountName"),
        ThreadPoolExecutor.CallerRunsPolicy(),
    )

    private val effectiveParallel = maxOf(
        1,
        minOf(parallelRequests.toLong(), rateLimitPerSec.toLong() * requestAverageProcessingTime.toMillis() / 1000).toInt()
    )

    private val client = OkHttpClient.Builder()
        // The adapter handles 408 so each retry acquires its own rate permit.
        .retryOnConnectionFailure(false)
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .connectionPool(ConnectionPool(effectiveParallel * 2, 5, TimeUnit.MINUTES))
        .build()

    private val inSystem = AtomicInteger()
    private val rateGranted = AtomicInteger()

    private val providerRetries = Counter.builder("payment.provider.retries")
        .description("Repeated payment requests submitted to the HTTP client")
        .tags("service", serviceName, "account", accountName)
        .register(meterRegistry)

    private val responseLatency = providerLatency(meterRegistry, "response")
    private val failedCallLatency = providerLatency(meterRegistry, "failure")

    private fun providerLatency(meterRegistry: MeterRegistry, outcome: String): Timer =
        Timer.builder("payment.provider.duration")
            .tags("service", serviceName, "account", accountName, "outcome", outcome)
            .publishPercentiles(0.95)
            .distributionStatisticExpiry(Duration.ofSeconds(30))
            .register(meterRegistry)

    private val defaultProcessingEstimateMs = (requestAverageProcessingTime.toMillis() * COLD_START_LATENCY_FACTOR).toLong()

    @Volatile
    private var processingEstimateMs = defaultProcessingEstimateMs

    private val processingEstimateJob = CoroutineScope(Dispatchers.Default).launch {
        while (isActive) {
            delay(LATENCY_ESTIMATE_REFRESH_MS.milliseconds)
            val snapshot = responseLatency.takeSnapshot()
            val p95 = snapshot.percentileValues().firstOrNull()?.value(TimeUnit.MILLISECONDS)
            processingEstimateMs = if (snapshot.count() >= MIN_LATENCY_SAMPLES && p95 != null && p95 > 0) {
                maxOf(requestAverageProcessingTime.toMillis(), p95.toLong())
            } else {
                defaultProcessingEstimateMs
            }
        }
    }

    override fun tryReserve(deadline: Long): Boolean {
        val position = inSystem.incrementAndGet()
        val processingMs = processingEstimateMs.coerceAtLeast(1)
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

    private fun waitBudget(deadline: Long) = deadline - now() - processingEstimateMs

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

            if (!awaitRate(deadline)) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Payment deadline exceeded.")
                }
                return
            }
            rateGranted.incrementAndGet()
            granted = true

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
            val sentAtNanos = System.nanoTime()
            val remainingMs = deadline - now()
            if (remainingMs <= 0) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Payment deadline exceeded.")
                }
                return
            }

            val attemptMs = (processingEstimateMs * ESTIMATE_SAFETY_FACTOR).toLong()
            val providerTimeoutMs = minOf(attemptMs, remainingMs - PROVIDER_RESPONSE_GRACE_MS).coerceAtLeast(1)
            val call = client.newCall(paymentRequest(paymentId, amount, transactionId, Duration.ofMillis(providerTimeoutMs)))
            call.timeout().deadlineNanoTime(sentAtNanos + TimeUnit.MILLISECONDS.toNanos(remainingMs))
            call.timeout().timeout(providerTimeoutMs + PROVIDER_RESPONSE_GRACE_MS, TimeUnit.MILLISECONDS)

            call.enqueue(object : Callback {
                override fun onResponse(call: Call, response: Response) {
                    var attemptReleased = false
                    var retryScheduled = false
                    try {
                        val body = try {
                            response.use { readResponse(it, transactionId, paymentId) }
                        } catch (e: Exception) {
                            failedCallLatency.record(System.nanoTime() - sentAtNanos, TimeUnit.NANOSECONDS)
                            throw e
                        }
                        val providerTimedOut = response.code == HttpStatus.REQUEST_TIMEOUT.value() &&
                            !body.result && body.message == "Timeout" &&
                            body.transactionId == transactionId.toString() && body.paymentId == paymentId.toString()
                        val latency = if (providerTimedOut) failedCallLatency else responseLatency
                        latency.record(System.nanoTime() - sentAtNanos, TimeUnit.NANOSECONDS)
                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                        if (!body.result && (response.isSuccessful || providerTimedOut) && retriesRemaining > 0 && waitBudget(deadline) > 0) {
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
                    try {
                        failedCallLatency.record(System.nanoTime() - sentAtNanos, TimeUnit.NANOSECONDS)
                        handleFailure(e, transactionId, paymentId)
                    } finally {
                        complete()
                    }
                }
            })
            handedOff = true
            if (retriesRemaining < MAX_RETRIES) providerRetries.increment()
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
        processingEstimateJob.cancel()
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

    private fun paymentRequest(paymentId: UUID, amount: Int, transactionId: UUID, timeout: Duration): Request {
        val url = "http://$paymentProviderHostPort/external/process".toHttpUrl().newBuilder()
            .addQueryParameter("serviceName", serviceName)
            .addQueryParameter("token", token)
            .addQueryParameter("accountName", accountName)
            .addQueryParameter("transactionId", transactionId.toString())
            .addQueryParameter("paymentId", paymentId.toString())
            .addQueryParameter("amount", amount.toString())
            .addQueryParameter("timeout", timeout.toString())
            .build()
        return Request.Builder().url(url).post(emptyBody).build()
    }

    private fun readResponse(response: Response, transactionId: UUID, paymentId: UUID): ExternalSysResponse {
        val responseBody = response.body?.string()
        return try {
            mapper.readValue(responseBody, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $responseBody")
            throw e
        }
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
