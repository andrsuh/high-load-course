package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.atomic.AtomicInteger
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.PacedRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
    private val client: OkHttpClient = OkHttpClient.Builder()
        .retryOnConnectionFailure(false).readTimeout(Duration.ZERO).build(),
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
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
    
    private fun recordWait(startedAt: Long, outcome: String) {
        meterRegistry.timer("payment.queue.wait", *tags, "outcome", outcome)
            .record(Duration.ofMillis((now() - startedAt).coerceAtLeast(0)))
    }

    init {
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
                return
            }

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            var httpStarted: Long? = null
            val dispatchClient = client.newBuilder().addNetworkInterceptor { chain ->
                val rateAcquired = rateLimiter.tickBlocking(Duration.ofMillis(latestStart - now()))
                if (!rateAcquired || now() >= latestStart) throw DispatchDeadlineExceeded()
                recordWait(paymentStartedAt, "sent")
                providerStarted = true
                httpStarted = System.nanoTime()
                inFlight.incrementAndGet()
                chain.proceed(chain.request())
            }.followRedirects(false).followSslRedirects(false).build()
            val body = try {
                dispatchClient.newCall(request).execute().use { response ->
                    // Consume the body once; malformed responses still release the slot.
                    val rawBody = response.body?.string()
                    try {
                        mapper.readValue(rawBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false,
                            "Invalid provider response (HTTP ${response.code})")
                    }
                }
            } finally {
                httpStarted?.let { started ->
                    httpTimer.record(System.nanoTime() - started, java.util.concurrent.TimeUnit.NANOSECONDS)
                    inFlight.decrementAndGet()
                }
            }
            if (!body.result) logger.warn("[$accountName] Provider rejected payment $paymentId: ${body.message}")
            providerCompleted = true
            val processedAt = now()

            // The response has been fully consumed. Persistence must not occupy provider capacity. so we can release the window now.
            ongoingWindow.release()
            windowAcquired = false
            count(if (body.result) {
                if (processedAt > deadline) "success_after_deadline" else "success_on_time"
            } else "provider_failure")
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, processedAt, transactionId, reason = body.message)
            }
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
    }

    override fun ratePerSecond(): Int = rateLimitPerSec

    override fun parallelCapacity(): Int = parallelRequests.coerceAtLeast(1)

    override fun latestStart(deadline: Long) = deadline - requestAverageProcessingTime.toMillis()

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
