package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.properties.RetryProperties
import java.net.SocketTimeoutException
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors
import java.util.stream.IntStream


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val rateLimiter: RateLimiter,
    private val ongoingWindow: NonBlockingOngoingWindow,
    private val retryProperties: RetryProperties,
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
    private val executor = Executors.newCachedThreadPool()
    private val scheduler = Executors.newScheduledThreadPool(1)

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        var attemptCount = 0

        while (attemptCount < retryProperties.count) {
            ++attemptCount
            if (isDeadlined(deadline)) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                return
            }

            val processed = AtomicBoolean(false)
            val mainFuture = CompletableFuture<Pair<Boolean, String>>()
            val hedgedFuture = CompletableFuture<Pair<Boolean, String>>()

            executor.submit {
                try {
                    val result = sendRequest(paymentId, transactionId, amount, deadline)
                    mainFuture.complete(result)
                } catch (e: Exception) {
                    mainFuture.completeExceptionally(e)
                }
            }

            scheduler.schedule({
                if (!mainFuture.isDone) {
                    executor.submit {
                        try {
                            val result = sendRequest(paymentId, transactionId, amount, deadline)
                            hedgedFuture.complete(result)
                        } catch (e: Exception) {
                            hedgedFuture.completeExceptionally(e)
                        }
                    }
                } else {
                    hedgedFuture.cancel(false)
                }
            }, retryProperties.hedgedDelayLimitMs, TimeUnit.MILLISECONDS)

            try {
                val result = CompletableFuture.anyOf(mainFuture, hedgedFuture).get() as Pair<Boolean, String>
                if (processed.compareAndSet(false, true)) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(result.first, now(), transactionId, reason = result.second)
                    }
                    if (result.first) {
                        mainFuture.cancel(true)
                        hedgedFuture.cancel(true)
                        return
                    }
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Error processing payment $paymentId: ${e.message}")
            }
            mainFuture.cancel(true)
            hedgedFuture.cancel(true)
        }
    }

    private fun sendRequest(
        paymentId: UUID,
        transactionId: UUID,
        amount: Int,
        deadline: Long
    ): Pair<Boolean, String> {
        if (isDeadlined(deadline)) {
            val reason = "Request timeout."
            logger.error("[$accountName] Deadline exceeded for payment $paymentId, txId: $transactionId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason)
            }
            return Pair(false, reason)
        }

        while (!rateLimiter.tick()) {
            Thread.sleep(10)
        }

        while (true) {
            if (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Success) {
                break
            }
        }

        try {
            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
                build()
            }
            client.newCall(request).execute().use { response ->
                val (success, message) = getResponse(response, transactionId, paymentId)
                return Pair(success, message)
            }
        } catch (e: Exception) {
            val message = when (e) {
                is SocketTimeoutException -> "Request timeout."
                else -> e.message ?: "Unknown error"
            }
            return Pair(false, message)
        } finally {
            ongoingWindow.releaseWindow()
        }
    }

    private fun getResponse(
        response: Response,
        transactionId: UUID,
        paymentId: UUID
    ): Pair<Boolean, String> {
        val body = try {
            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        val success = IntStream.rangeClosed(200, 299)
            .boxed()
            .collect(Collectors.toSet())
            .contains(response.code) && body.result

        return Pair(
            success,
            body.message ?: if (success) "Success" else "Failed"
        )
    }

    private fun isDeadlined(deadline: Long): Boolean = LocalDateTime.now().isAfter(
        Instant.ofEpochMilli(deadline).atZone(ZoneId.systemDefault()).toLocalDateTime()
    )

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()