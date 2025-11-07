package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.batch.BatchDataSource
import org.springframework.context.annotation.Bean
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import ru.quipy.metrics.MetricsService


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metricsService: MetricsService
) : PaymentExternalSystemAdapter {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000));
    private val semaphore = Semaphore(parallelRequests)

    private val client = OkHttpClient.Builder().build()

    @Bean
    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val start = now()
        val avgMs = requestAverageProcessingTime.toMillis()
        val remaining = deadline - start
        val maxRetries = 5
        val timeout = 500L

        logger.info("[$accountName] Submitting payment $paymentId (txId=$transactionId), remaining=${remaining}ms")

        // Always record submission
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(start - paymentStartedAt))
        }

        if (remaining < avgMs) {
            logger.warn("[$accountName] Skipping payment $paymentId: remaining ${remaining}ms < avg ${avgMs}ms")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline too close")
            }
            return
        }

        var attempt = 0
        var success = false
        var lastReason: String? = null

        while (attempt < maxRetries && !success) {
            attempt++

            if (attempt > 1) {
                metricsService.increaseRetryCounter()
            }

            val attemptStart = now()
            val timeLeft = deadline - attemptStart

            if (timeLeft < avgMs) {
                logger.warn("[$accountName] Stop retrying payment $paymentId: deadline too close (${timeLeft}ms left)")
                break
            }

            semaphore.acquire()
            if (!rateLimiter.tick()) {
                val retryDelay = timeout * attempt
                logger.warn("[$accountName] Rate limited (attempt $attempt) delaying $retryDelay ms")
                semaphore.release()
                Thread.sleep(retryDelay)
                continue
            }

            try {
                val request = Request.Builder()
                    .url(
                        "http://$paymentProviderHostPort/external/process" +
                                "?serviceName=$serviceName" +
                                "&token=$token" +
                                "&accountName=$accountName" +
                                "&transactionId=$transactionId" +
                                "&paymentId=$paymentId" +
                                "&amount=$amount"
                    )
                    .post(emptyBody)
                    .build()

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Failed to parse response: ${e.message}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.info("CODE: ${response.code}")

                    if (response.isSuccessful && body.result) {
                        logger.info("[$accountName] Payment success for txId=$transactionId, payment=$paymentId")
                        success = true
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = body.message)
                        }
                    } else {
                        val retriable = isRetriable(null, response.code, body.message)
                        lastReason = "HTTP ${response.code}: ${body.message}"
                        logger.warn("[$accountName] Payment failed (attempt $attempt/$maxRetries): $lastReason, retriable=$retriable")

                        if (!retriable) break

                        val backoff = (timeout * attempt).coerceAtMost(timeLeft / 2)
                        Thread.sleep(backoff)
                    }
                }
            } catch (e: Exception) {
                val retriable = isRetriable(e, null, e.message)
                lastReason = e.message
                logger.error("[$accountName] Exception during payment $paymentId: ${e.javaClass.simpleName}, retriable=$retriable", e)

                if (!retriable) break

                val backoff = (timeout * attempt).coerceAtMost((deadline - now()) / 2)
                Thread.sleep(backoff)
            } finally {
                semaphore.release()
            }
        }

        if (!success) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = lastReason ?: "Permanent failure")
            }
        }
    }



    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

private fun isRetriable(e: Exception?, code: Int?, message: String?): Boolean {
    if (e is SocketTimeoutException) return true
    if (e is java.net.ConnectException) return true
    if (e is java.net.SocketException) return true

    if(message?.contains("Temporary error", ignoreCase = true) == true) return true

    if (code != null) {
        return when (code) {
            429,
            in 500..599 -> true
            else -> false
        }
    }

    if (message?.contains("timeout", ignoreCase = true) == true) return true
    return false
}


public fun now() = System.currentTimeMillis()