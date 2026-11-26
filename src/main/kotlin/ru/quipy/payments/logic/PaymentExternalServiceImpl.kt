package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


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
        const val MAX_ATTEMPTS = 3
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val submissionRate = rateLimitPerSec.coerceAtLeast(1)
    private val parallelRequests = properties.parallelRequests
    private val pendingPayments = AtomicInteger()
    private val processingBudgetMillis = requestAverageProcessingTime.toMillis()
    private val requestTimeoutMillis = when (accountName) {
        "acc-7" -> processingBudgetMillis * 3 / 2
        else -> processingBudgetMillis * 2
    }
    private val clientTimeoutMillis = requestTimeoutMillis + 250
    private val shortProviderTailMillis = processingBudgetMillis + 500
    private val providerTailMillis = processingBudgetMillis * 2
    private val burstProviderTailMillis = processingBudgetMillis * 3
    private val shortDeadlineMillis = 3_000L
    private val burstDeadlineMillis = 20_000L
    private val burstQueueBudgetMillis = 20_000L
    private val burstQueueCapacity = submissionRate * burstQueueBudgetMillis / 1_000
    private val shortQueueCapacity = submissionRate
    private val dispatchRateLimiter = SlidingWindowRateLimiter(submissionRate.toLong(), Duration.ofMillis(1_025))
    private val retryAfterMillis = (1_000L + submissionRate - 1) / submissionRate
    private val nextRetryAt = AtomicLong()
    private val admissionRateLimiter = TokenBucketRateLimiter(
        submissionRate,
        parallelRequests,
        1,
        TimeUnit.SECONDS
    )
    private val requestLatency = DistributionSummary.builder("payment_external_request_duration_milliseconds")
        .description("External payment request duration")
        .baseUnit("milliseconds")
        .minimumExpectedValue(1.0)
        .maximumExpectedValue(requestTimeoutMillis.toDouble())
        .publishPercentiles(0.5, 0.85, 0.9, 0.95, 0.99)
        .publishPercentileHistogram()
        .tags("service", serviceName, "accountName", accountName)
        .register(meterRegistry)
    private val retryCounter = Counter.builder("payment_external_retries")
        .description("External payment retries")
        .tags("service", serviceName, "accountName", accountName)
        .register(meterRegistry)
    private val timeoutCounter = Counter.builder("payment_external_timeouts")
        .description("External payment call timeouts")
        .tags("service", serviceName, "accountName", accountName)
        .register(meterRegistry)

    override fun tryReserve(paymentId: UUID, deadline: Long): Long? {
        val now = now()
        if (deadline - now <= shortDeadlineMillis) {
            val maxPending = ((deadline - now - shortProviderTailMillis) * submissionRate / 1_000)
                .coerceIn(0L, shortQueueCapacity.toLong())
            while (true) {
                val pending = pendingPayments.get()
                if (pending.toLong() >= maxPending) return shortRetryAt(paymentId, now)
                if (pendingPayments.compareAndSet(pending, pending + 1)) return null
            }
        }
        val processingReserve = if (deadline - now >= burstDeadlineMillis) burstProviderTailMillis else providerTailMillis
        val deadlineCapacity = ((deadline - now - processingReserve) * submissionRate / 1_000).coerceAtLeast(1)
        val maxPending = if (deadline - now >= burstDeadlineMillis) {
            minOf(deadlineCapacity, burstQueueCapacity)
        } else {
            deadlineCapacity
        }
        while (true) {
            val pending = pendingPayments.get()
            if (pending.toLong() >= maxPending) return reserveRetryAt(now)
            if (pendingPayments.compareAndSet(pending, pending + 1)) break
        }
        if (deadline - now >= burstDeadlineMillis || admissionRateLimiter.tick()) return null
        pendingPayments.decrementAndGet()
        return reserveRetryAt(now)
    }

    private fun reserveRetryAt(now: Long, earliest: Long = now + retryAfterMillis): Long {
        while (true) {
            val current = nextRetryAt.get()
            val retryAt = maxOf(earliest, current)
            if (nextRetryAt.compareAndSet(current, retryAt + retryAfterMillis)) return retryAt
        }
    }

    private fun shortRetryAt(paymentId: UUID, now: Long): Long {
        val slot = Math.floorMod(paymentId.hashCode(), submissionRate)
        val jitterStepMillis = maxOf(10L, retryAfterMillis / 4)
        return now + jitterStepMillis * (slot + 1L)
    }

    override fun releaseReservation(paymentId: UUID) {
        pendingPayments.decrementAndGet()
    }

    private val rateLimiter = LeakingBucketRateLimiter(
        submissionRate.toLong(),
        Duration.ofSeconds(1),
        parallelRequests
    )
    private val window = OngoingWindow(parallelRequests)

    private val client = OkHttpClient.Builder()
        .connectTimeout(minOf(1_000L, requestTimeoutMillis), TimeUnit.MILLISECONDS)
        .readTimeout(clientTimeoutMillis, TimeUnit.MILLISECONDS)
        .writeTimeout(minOf(1_000L, requestTimeoutMillis), TimeUnit.MILLISECONDS)
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        performPaymentAttempt(paymentId, amount, paymentStartedAt, deadline, 1)
    }

    private fun performPaymentAttempt(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int,
    ) {
        var dispatchPermit: SlidingWindowRateLimiter.Permit? = null
        var slotAcquired = false
        var submitted = false
        val transactionId = UUID.randomUUID()
        val burstMode = deadline - paymentStartedAt >= burstDeadlineMillis
        val shortMode = deadline - paymentStartedAt <= shortDeadlineMillis
        val processingReserve = when {
            burstMode -> burstProviderTailMillis
            shortMode -> shortProviderTailMillis
            else -> providerTailMillis
        }
        val latestSubmissionAt = deadline - processingReserve

        try {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            if (now() > latestSubmissionAt) {
                logger.warn("[$accountName] Skip before throttle, deadline would be missed. payment: $paymentId")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before submit")
                }
                return
            }

            if (burstMode || shortMode) {
                dispatchPermit = dispatchRateLimiter.reserveBlockingUntil(latestSubmissionAt)
                if (dispatchPermit == null) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded waiting for rate permit")
                    }
                    return
                }
            } else {
                if (!rateLimiter.reserveBlockingUntil(latestSubmissionAt)) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded waiting for rate permit")
                    }
                    return
                }
            }

            if (!window.tryAcquire(latestSubmissionAt - now())) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded waiting for concurrent request slot")
                }
                return
            }
            slotAcquired = true

            if (now() > latestSubmissionAt) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before HTTP submit")
                }
                return
            }

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${Duration.ofMillis(requestTimeoutMillis)}")
                post(emptyBody)
            }.build()

            val requestStartedAt = now()
            val call = client.newCall(request)
            val callTimeoutMillis = minOf(clientTimeoutMillis, deadline - requestStartedAt)
            if (callTimeoutMillis <= 0) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before HTTP call")
                }
                return
            }
            call.timeout().timeout(callTimeoutMillis, TimeUnit.MILLISECONDS)
            dispatchPermit?.markSubmitted()
            call.enqueue(object : Callback {
                override fun onResponse(call: Call, response: Response) {
                    var retry = false
                    try {
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        retry = !body.result &&
                            (body.message == "Temporary error" || body.message == "Timeout" || response.code == 408 || response.code == 429 || response.code >= 500) &&
                            canRetry(attempt, deadline)
                        if (body.message == "Timeout" || response.code == 408) timeoutCounter.increment()
                        if (!retry) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                        }
                    } finally {
                        requestLatency.record((now() - requestStartedAt).toDouble())
                        response.close()
                        window.release()
                    }
                    if (retry) retry(paymentId, amount, paymentStartedAt, deadline, attempt)
                }

                override fun onFailure(call: Call, e: IOException) {
                    val retry = canRetry(attempt, deadline)
                    try {
                        if (e is SocketTimeoutException || call.isCanceled()) timeoutCounter.increment()
                        if (!retry) {
                            val reason = if (e is SocketTimeoutException) "Request timeout." else e.message
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = reason)
                            }
                        }
                    } finally {
                        requestLatency.record((now() - requestStartedAt).toDouble())
                        window.release()
                    }
                    if (retry) retry(paymentId, amount, paymentStartedAt, deadline, attempt)
                }
            })
            submitted = true
        } finally {
            dispatchPermit?.close()
            if (slotAcquired && !submitted) window.release()
        }
    }

    private fun retry(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, attempt: Int) {
        retryCounter.increment()
        performPaymentAttempt(paymentId, amount, paymentStartedAt, deadline, attempt + 1)
    }

    private fun canRetry(attempt: Int, deadline: Long): Boolean =
        attempt < MAX_ATTEMPTS && now() + requestAverageProcessingTime.toMillis() < deadline

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
