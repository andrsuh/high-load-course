package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    @Autowired private val metricsReporter: MetricsReporter
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests

    private val semaphore = Semaphore(parallelRequests)
    private val client = OkHttpClient.Builder()
        .connectTimeout(2000, TimeUnit.MILLISECONDS)
        .readTimeout(1600, TimeUnit.MILLISECONDS)
        .writeTimeout(1600, TimeUnit.MILLISECONDS)
        .callTimeout(2500, TimeUnit.MILLISECONDS)
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var attempt = 0
        val maxAttempts = 3

        while (attempt < maxAttempts) {
            attempt++

            val remainingTime = deadline - System.currentTimeMillis()
            if (remainingTime < 200) {
                recordFinalFailure(paymentId, paymentStartedAt, "Insufficient time: ${remainingTime}ms")
                return
            }

            val acquired = semaphore.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)
            if (!acquired) {
                recordFinalFailure(paymentId, paymentStartedAt, "Semaphore timeout")
                return
            }

            val transactionId = UUID.randomUUID()
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            try {

                val callTimeout = minOf(remainingTime - 100, 2500)

                metricsReporter.updateCurrentTimeout(accountName, callTimeout.toLong())

                val callClient = client.newBuilder()
                    .callTimeout(callTimeout, TimeUnit.MILLISECONDS)
                    .readTimeout(callTimeout, TimeUnit.MILLISECONDS)
                    .writeTimeout(callTimeout, TimeUnit.MILLISECONDS)
                    .connectTimeout(minOf(callTimeout, 2000), TimeUnit.MILLISECONDS)
                    .build()

                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                callClient.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Parse error for payment $paymentId")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Parse error")
                    }

                    if (response.code == 200 && body.result) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = body.message)
                        }
                        return
                    }

                    val shouldRetry = response.code == 429 || response.code in 500..599

                    val cause = if (response.code == 429) RetryCause.HTTP_429 else RetryCause.HTTP_5XX
                    metricsReporter.incrementRetry(accountName, cause)

                    if (!shouldRetry || attempt == maxAttempts) {
                        recordProcessingFailure(paymentId, transactionId, "HTTP ${response.code}: ${body.message}")
                        return
                    }

                    // Ретрай после паузы
                    val backoff = when (attempt) {
                        1 -> 50L
                        2 -> 100L
                        else -> 200L
                    }
                    if (remainingTime > backoff + 200) {
                        Thread.sleep(backoff)
                    }
                }
            } catch (e: Exception) {
                val shouldRetry = e is SocketTimeoutException && attempt < maxAttempts
                if (!shouldRetry) {
                    recordProcessingFailure(paymentId, transactionId,
                        if (e is SocketTimeoutException) "Timeout" else e.message ?: "Error")
                    return
                }

                metricsReporter.incrementRetry(accountName, RetryCause.TIMEOUT)
                logger.info("[$accountName] Retry attempt ${attempt + 1} for payment $paymentId (cause=TIMEOUT)")

                // Ретрай для таймаута без паузы
            } finally {
                semaphore.release()
            }
        }
    }

    private fun recordProcessingFailure(paymentId: UUID, transactionId: UUID, reason: String) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    private fun recordFinalFailure(paymentId: UUID, paymentStartedAt: Long, reason: String) {
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()