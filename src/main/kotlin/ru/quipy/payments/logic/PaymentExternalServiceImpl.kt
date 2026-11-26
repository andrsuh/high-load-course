package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.io.InterruptedIOException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.NANOSECONDS
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.PacedRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.time.Duration
import java.util.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
    private val maxAttempts: Int,
    private val retryDelayMillis: Long,
    private val resultPersistenceReserveMillis: Long,
    private val client: OkHttpClient = OkHttpClient.Builder()
        .retryOnConnectionFailure(false).readTimeout(Duration.ZERO).build(),
    private val providerTimeoutCapMillis: Long = 0,
    private val localTimeoutRetry: Boolean = false,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        private const val PROVIDER_RESPONSE_RESERVE_MILLIS = 100L
    }

    private class DispatchDeadlineExceeded : java.io.IOException("Payment dispatch deadline exceeded")

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = PacedRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val tags = arrayOf("service", serviceName, "account", accountName)
    private val inFlight = AtomicInteger()
    private val httpTimer = meterRegistry.timer("payment.provider.duration", *tags)
    private val successDuration = Timer.builder("payment.provider.success.duration")
        .tags(*tags).publishPercentileHistogram().register(meterRegistry)
    private val dispatchBudgets = listOf("success", "failure", "timeout", "provider_timeout", "unknown").associateWith { outcome ->
        Timer.builder("payment.provider.dispatch.budget")
            .tags(*tags, "outcome", outcome).publishPercentileHistogram().register(meterRegistry)
    }
    
    private fun recordWait(startedAt: Long, outcome: String) {
        meterRegistry.timer("payment.queue.wait", *tags, "outcome", outcome)
            .record(Duration.ofMillis((now() - startedAt).coerceAtLeast(0)))
    }

    init {
        require(maxAttempts > 0)
        require(retryDelayMillis >= 0)
        require(resultPersistenceReserveMillis >= 0)
        require(providerTimeoutCapMillis >= 0)
        if (localTimeoutRetry) {
            require(URI("http://$paymentProviderHostPort").host in setOf("localhost", "127.0.0.1", "[::1]"))
            require(providerTimeoutCapMillis > 0)
            logger.info("[$accountName] Local simulator timeout retry enabled")
        }
        if (providerTimeoutCapMillis > 0) {
            logger.info("[$accountName] Provider timeout cap: ${providerTimeoutCapMillis}ms")
        }
        listOf("expired_before_send", "success_on_time", "success_after_deadline",
            "provider_failure", "provider_error_unknown").forEach {
            meterRegistry.counter("payment.attempts", *tags, "outcome", it)
        }
        meterRegistry.gauge("payment.provider.inflight", io.micrometer.core.instrument.Tags.of(*tags), inFlight)
        io.micrometer.core.instrument.Gauge.builder("payment.admission.waiters", ongoingWindow) {
            it.awaitingQueueSize().toDouble()
        }.tags(*tags).register(meterRegistry)
    }

    private fun count(outcome: String) = meterRegistry.counter("payment.attempts", *tags, "outcome", outcome).increment()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val attemptLimit = if (localTimeoutRetry) maxAttempts.coerceAtMost(2) else maxAttempts
        for (attempt in 1..attemptLimit) {
            val retryable = performAttempt(paymentId, amount, paymentStartedAt, deadline, attempt > 1)
            if (!retryable || attempt == attemptLimit) return

            if (latestStart(deadline) - now() <= retryDelayMillis) return
            try {
                Thread.sleep(retryDelayMillis)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }
            logger.info("[$accountName] Retrying payment $paymentId")
        }
    }

    private fun performAttempt(
        paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, isRetry: Boolean
    ): Boolean {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        var windowAcquired = false
        var providerStarted = false
        var providerCompleted = false
        try {

            val latestStart = latestStart(deadline)
            val acquired = try {
                windowAcquired = ongoingWindow.acquire(Duration.ofMillis(latestStart - now()))
                windowAcquired && now() < latestStart
                } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }

            val submittedAt = now()
            paymentESService.update(paymentId) {
                it.logSubmission(acquired, transactionId, submittedAt, Duration.ofMillis(submittedAt - paymentStartedAt))
            }
            
            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
            
            if (!acquired) {
                recordWait(paymentStartedAt, "discarded")
                count("expired_before_send")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Outdated payment")
                }
                return false
            }

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            var httpStarted: Long? = null
            var dispatchBudgetNanos = 0L
            var callStarted = 0L
            var callBudget = 0L
            var httpOutcome = "unknown"
            var providerTimedOut = false
            val dispatchClient = client.newBuilder().addNetworkInterceptor { chain ->
                val rateAcquired = rateLimiter.tickBlocking(Duration.ofMillis(latestStart - now()))
                if (!rateAcquired || now() >= latestStart) throw DispatchDeadlineExceeded()
                val started = System.nanoTime()
                val remaining = MILLISECONDS.toNanos(callBudget) - (started - callStarted)
                val providerBudget = NANOSECONDS.toMillis(remaining) - PROVIDER_RESPONSE_RESERVE_MILLIS
                if (providerBudget <= 0) throw DispatchDeadlineExceeded()
                if (localTimeoutRetry && isRetry && providerBudget < providerTimeoutCapMillis) {
                    throw DispatchDeadlineExceeded()
                }
                val providerTimeout = if (providerTimeoutCapMillis > 0) {
                    minOf(providerBudget, providerTimeoutCapMillis)
                } else {
                    providerBudget
                }
                val outgoing = chain.request()
                val url = outgoing.url.newBuilder()
                    .addQueryParameter("timeout", Duration.ofMillis(providerTimeout).toString())
                    .build()
                recordWait(paymentStartedAt, "sent")
                providerStarted = true
                httpStarted = started
                dispatchBudgetNanos = remaining
                inFlight.incrementAndGet()
                chain.proceed(outgoing.newBuilder().url(url).build())
            }.followRedirects(false).followSslRedirects(false).build()
            val body = try {
                callBudget = deadline - now() - resultPersistenceReserveMillis
                if (callBudget <= 0) throw DispatchDeadlineExceeded()
                val call = dispatchClient.newCall(request)
                call.timeout().timeout(callBudget, MILLISECONDS)
                callStarted = System.nanoTime()
                call.execute().use { response ->
                    providerTimedOut = response.code == 408
                    // Consume the body once; malformed responses still release the slot.
                    val rawBody = response.body?.string()
                    try {
                        mapper.readValue(rawBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false,
                            "Invalid provider response (HTTP ${response.code})")
                    }
                }.also {
                    httpOutcome = when {
                        providerTimedOut -> "provider_timeout"
                        it.result -> "success"
                        else -> "failure"
                    }
                }
            } catch (e: InterruptedIOException) {
                if (e is SocketTimeoutException || e.message == "timeout") httpOutcome = "timeout"
                throw e
            } finally {
                httpStarted?.let { started ->
                    inFlight.decrementAndGet()
                    val elapsed = System.nanoTime() - started
                    httpTimer.record(elapsed, NANOSECONDS)
                    dispatchBudgets.getValue(httpOutcome).record(dispatchBudgetNanos, NANOSECONDS)
                    if (httpOutcome == "success") successDuration.record(elapsed, NANOSECONDS)
                }
            }
            providerCompleted = true
            val processedAt = now()

            // The response has been fully consumed. Persistence must not occupy provider capacity. so we can release the window now.
            ongoingWindow.release()
            windowAcquired = false
            if (providerTimedOut) {
                logger.warn("[$accountName] Provider reported timeout for payment $paymentId, txId: $transactionId")
                count("provider_error_unknown")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, processedAt, transactionId, reason = "Provider timeout. Payment outcome unknown.")
                }
                return localTimeoutRetry && !isRetry && !body.result && body.message == "Timeout" &&
                    body.transactionId == transactionId.toString() && body.paymentId == paymentId.toString() &&
                    deadline - now() - retryDelayMillis - resultPersistenceReserveMillis -
                    PROVIDER_RESPONSE_RESERVE_MILLIS >= providerTimeoutCapMillis
            }
            if (!body.result) logger.warn("[$accountName] Provider rejected payment $paymentId: ${body.message}")
            count(if (body.result) {
                if (processedAt > deadline) "success_after_deadline" else "success_on_time"
            } else "provider_failure")
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, processedAt, transactionId, reason = body.message)
            }
            return !body.result && body.message?.trim()?.equals("Temporary error", ignoreCase = true) == true
        } catch (e: Exception) {
            if (providerStarted && !providerCompleted) count("provider_error_unknown")
            when (e) {
                is DispatchDeadlineExceeded -> {
                    recordWait(paymentStartedAt, "discarded")
                    count("expired_before_send")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "There is no time for payment")
                    }
                }
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
            if (windowAcquired) ongoingWindow.release()
        }
        return false
    }

    override fun ratePerSecond(): Int = rateLimitPerSec

    override fun parallelCapacity(): Int = parallelRequests.coerceAtLeast(1)

    override fun latestStart(deadline: Long) = deadline - requestAverageProcessingTime.toMillis()

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
