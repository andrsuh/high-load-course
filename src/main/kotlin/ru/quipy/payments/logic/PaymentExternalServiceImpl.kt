package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.api.Result
import org.eclipse.jetty.client.util.BufferingResponseListener
import org.eclipse.jetty.client.util.BytesContentProvider
import org.eclipse.jetty.http2.client.HTTP2Client
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.random.Random

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()

        private const val MAX_RETRIES = 3
        private const val INITIAL_RETRY_DELAY_MS = 500L
        private const val JITTER_MS = 300L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val parallelRequests = properties.parallelRequests

    // Jetty HttpClient с поддержкой HTTP/2
    private val client = HttpClient(
        HttpClientTransportOverHTTP2(HTTP2Client()),
        SslContextFactory.Client()
    ).apply {
        connectTimeout = 10_000L
        start()
    }

    private val semaphore = Semaphore(parallelRequests)
    private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        processPayment(paymentId, amount, transactionId, attempt = 1, deadline = deadline)
    }

    private fun processPayment(paymentId: UUID, amount: Int, transactionId: UUID, attempt: Int, deadline: Long) {
        if (now() > deadline) {
            logger.error("[$accountName] Deadline exceeded for payment $paymentId, txId: $transactionId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
            }
            return
        }

        semaphore.acquire()
        try {
            val requestUrl = "http://localhost:1234/external/process?" +
                    "serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&" +
                    "paymentId=$paymentId&amount=$amount"

            val request = client.newRequest(requestUrl)
                .method("POST")
                .timeout(65, TimeUnit.SECONDS)
                .content(BytesContentProvider(ByteArray(0)))

            request.send(object : BufferingResponseListener(64 * 1024) {
                override fun onComplete(result: Result) {
                    semaphore.release()
                    if (!result.isSucceeded) {
                        val exception = result.failure
                        if (exception?.message?.contains("timed out") == true) {
                            logger.warn("[$accountName] Attempt $attempt timed out for payment $paymentId, txId: $transactionId")
                        } else {
                            logger.error("[$accountName] Attempt $attempt failed for payment $paymentId, txId: $transactionId", exception)
                        }
                        if (attempt < MAX_RETRIES && now() < deadline) {
                            val backoff = INITIAL_RETRY_DELAY_MS * (1L shl (attempt - 1)) + Random.nextLong(JITTER_MS)
                            logger.warn("[$accountName] Scheduling retry ${attempt + 1} after ${backoff}ms for payment $paymentId")
                            scheduler.schedule({
                                processPayment(paymentId, amount, transactionId, attempt + 1, deadline)
                            }, backoff, TimeUnit.MILLISECONDS)
                        } else {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = exception?.message)
                            }
                        }
                    } else {
                        val responseBody = contentAsString
                        val body = try {
                            mapper.readValue(responseBody, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] Error parsing response for txId: $transactionId, payment: $paymentId", e)
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }
                        logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    }
                }
            })
        } catch (ex: Exception) {
            semaphore.release()
            logger.error("[$accountName] Exception during processing payment $paymentId, txId: $transactionId", ex)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = ex.message)
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    fun shutdown() {
        scheduler.shutdown()
        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow()
            }
        } catch (e: InterruptedException) {
            scheduler.shutdownNow()
        }
        try {
            client.stop()
        } catch (e: Exception) {
            logger.error("Error stopping HttpClient", e)
        }
    }
}

fun now() = System.currentTimeMillis()
