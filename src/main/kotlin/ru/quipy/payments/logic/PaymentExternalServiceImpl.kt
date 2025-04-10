package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val rateLimiter = LeakingBucketRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1),
        bucketSize = rateLimitPerSec
    )

    private val deadlineOngoingWindow = DeadlineOngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (!deadlineOngoingWindow.acquire(deadline)) {
            logger.warn("[$accountName] Deadline exceeded, cancelling request, deadline: $deadline")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
            }
            deadlineOngoingWindow.release()
            return
        }

        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .header("x-idempotency-key", UUID.randomUUID().toString())
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        while (!rateLimiter.tick()) {
            Thread.sleep(5)
        }

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenAcceptAsync { response ->
                try {
                    val responseBody = mapper.readValue(response.body(), ExternalSysResponse::class.java)
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${responseBody.result}, message: ${responseBody.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(responseBody.result, now(), transactionId, reason = responseBody.message)
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                } finally {
                    deadlineOngoingWindow.release()
                }
            }
            .exceptionally { e ->
                try {
                    when (e.cause) {
                        is java.net.SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId)
                            {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                } finally {
                    deadlineOngoingWindow.release()
                }
                null
            }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()