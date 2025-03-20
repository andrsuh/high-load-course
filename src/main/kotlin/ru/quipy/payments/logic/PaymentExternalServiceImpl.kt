package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
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

    private val rateLimiter = LeakingBucketRateLimiter(
        rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1), bucketSize = rateLimitPerSec
    )

    private val deadlineOngoingWindow = DeadlineOngoingWindow(parallelRequests)

    private val initialBackoff = Duration.ofMillis(80.toLong())
    private val backoffCoefficient = 1.5
    private val maxRequestRetries = 3

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        var retryCount = 0;
        var backoff = initialBackoff;

        while (retryCount < maxRequestRetries) {
            try {
                val body = performHedgedRequest(client, request, mapper, transactionId, paymentId, accountName, requestAverageProcessingTime.toMillis() * 2, deadline)

                if (!body.result) {
                    logger.warn("[$accountName] [RETRY] txId: $transactionId, retryCount: $retryCount, backoff: $backoff")

                    Thread.sleep(backoff)
                    backoff = Duration.ofMillis((backoff.toMillis() * backoffCoefficient).toLong())
                    retryCount++

                    if (retryCount == maxRequestRetries) {
                        logger.warn("[$accountName] [RETRY] txId: $transactionId, retries didn't help")
                    }
                } else {
                    retryCount = 10000000
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            } catch (e: Exception) {
                when (e) {
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
                deadlineOngoingWindow.release()
            }
        }

    }

    private fun performHedgedRequest(
        client: OkHttpClient,
        request: Request,
        mapper: ObjectMapper,
        transactionId: UUID,
        paymentId: UUID,
        accountName: String,
        hedgedTimeoutMs: Long,
        deadline: Long
    ): ExternalSysResponse {
        val executor = Executors.newCachedThreadPool()
        val idempotencyKey = UUID.randomUUID().toString()
        val requestWithIdempotency = request.newBuilder()
            .header("x-idempotency-key", idempotencyKey)
            .build()

        return try {
            val firstCall = executor.submit<ExternalSysResponse> {
                executeRequest(client, requestWithIdempotency, mapper, transactionId, paymentId, accountName, deadline)
            }

            var secondCall: Future<ExternalSysResponse>? = null

            try {
                firstCall.get(hedgedTimeoutMs, TimeUnit.MILLISECONDS)
            } catch (e: TimeoutException) {
                logger.warn("[$accountName] First call exceeded ${hedgedTimeoutMs}ms, sending hedged request")
                secondCall = executor.submit<ExternalSysResponse> {
                    executeRequest(client, requestWithIdempotency, mapper, transactionId, paymentId, accountName, deadline)
                }

                CompletableFuture.anyOf(
                    CompletableFuture.supplyAsync { firstCall.get() },
                    CompletableFuture.supplyAsync { secondCall.get() }
                ).get() as ExternalSysResponse
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Hedged request failed for txId: $transactionId, payment: $paymentId", e)
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }
    }

    private fun executeRequest(
        client: OkHttpClient,
        request: Request,
        mapper: ObjectMapper,
        transactionId: UUID,
        paymentId: UUID,
        accountName: String,
        deadline: Long
    ): ExternalSysResponse {
        if (!deadlineOngoingWindow.acquire(deadline)) {
            logger.warn("[$accountName] Deadline exceeded, cancelling request, deadline: $deadline")

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
            }

            return ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "deadline bad")
        }

        try {
            while (!rateLimiter.tick()) {
                Thread.sleep(5)
            }

            return client.newCall(request).execute().use { response ->
                try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
            }
        } finally {
            deadlineOngoingWindow.release()
        }
    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()