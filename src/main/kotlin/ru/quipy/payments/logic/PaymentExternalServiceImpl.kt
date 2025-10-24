package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.ConnectionPool
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import ru.quipy.common.utils.NamedThreadFactory


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
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

    private val optimalThreads = maxOf(
        (rateLimitPerSec * requestAverageProcessingTime.seconds * 3).toInt(),
        parallelRequests
    )

    private val client = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(optimalThreads * 2, 5, TimeUnit.MINUTES))  // 128 connections для acc-23
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(10, TimeUnit.SECONDS)
        .build()

    private val parallelRequestSemaphore = Semaphore(optimalThreads)

    private val asyncExecutor = ThreadPoolExecutor(
        optimalThreads,
        optimalThreads * 2,
        60L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(500),
        NamedThreadFactory("payment-async-$accountName")
    )

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = (rateLimitPerSec * 1.05).toLong(),
        window = Duration.ofSeconds(1)
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, activeRequestsCount: java.util.concurrent.atomic.AtomicInteger) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        activeRequestsCount.incrementAndGet()

        asyncExecutor.submit {
            try {
                executeWithSemaphore(paymentId, transactionId, amount, deadline)
            } finally {
                activeRequestsCount.decrementAndGet()
            }
        }
    }

    private fun executeWithSemaphore(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long) {
        try {
            val now = now()

            if (now >= deadline) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline expired before semaphore")
                }
                return
            }

            val timeoutMs = deadline - now
            val acquired = parallelRequestSemaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)

            if (!acquired) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Semaphore timeout - deadline approaching")
                }
                return
            }

            if (now() >= deadline) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline expired during semaphore wait")
                }
                return
            }

            while (!rateLimiter.tick()) {
                if (now() >= deadline) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline expired while waiting for rate limit")
                    }
                    return
                }

                Thread.sleep(5)
            }

            executePaymentRequest(paymentId, transactionId, amount, deadline)
        } catch (e: InterruptedException) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Interrupted waiting for parallel slot")
            }
        } finally {
            parallelRequestSemaphore.release()
        }
    }

    private fun executePaymentRequest(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long, attempt: Int = 1) {
        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            client.newCall(request).execute().use { response ->
                // Обработка 429 (Too Many Requests) с проверкой deadline
                if (response.code == 429 && attempt <= 10) {
                    if (now() >= deadline) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "429 retry aborted - deadline expired")
                        }
                        return
                    }

                    Thread.sleep(20)
                    executePaymentRequest(paymentId, transactionId, amount, deadline, attempt + 1)
                    return
                }

                if (!response.isSuccessful && attempt <= 3) {
                    // ПРОВЕРКА deadline перед retry!
                    if (now() >= deadline) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Retry aborted - deadline expired")
                        }
                        return
                    }

                    Thread.sleep(50)
                    executePaymentRequest(paymentId, transactionId, amount, deadline, attempt + 1)
                    return
                }

                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    if (attempt <= 3 && now() < deadline) {
                        val delayMs = minOf(25 + (attempt * 10), 80).toLong()
                        Thread.sleep(delayMs)
                        executePaymentRequest(paymentId, transactionId, amount, deadline, attempt + 1)
                        return
                    }
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout after retries.")
                    }
                }

                else -> {

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()