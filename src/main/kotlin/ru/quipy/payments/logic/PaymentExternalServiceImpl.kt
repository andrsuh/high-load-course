package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val expectedProcessingMs = properties.averageProcessingTime.toMillis()
    private val processingSamples = ConcurrentLinkedDeque<Long>()
    private val maxProcessingSamples = 512

    private val parallelRequestsSemaphore = Semaphore(properties.parallelRequests, true)

    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val client = OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(5))
        .readTimeout(Duration.ZERO)
        .writeTimeout(Duration.ofSeconds(5))
        .build()

    override fun canAccept(deadline: Long, queuedRequests: Int): Boolean {
        val now = System.currentTimeMillis()
        val p99 = processingP99Millis()
        val batchesAhead = (queuedRequests + parallelRequestsSemaphore.queueLength + 1L +
            properties.parallelRequests - 1) / properties.parallelRequests
        val predictedCompletion = now + batchesAhead * p99
        val accepted = predictedCompletion < deadline
        return accepted
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        val now = System.currentTimeMillis()
        if (now >= deadline) {
            logger.warn("[$accountName] Payment $paymentId deadline ALREADY exceeded by ${now - deadline}ms")
            reject(paymentId, transactionId, paymentStartedAt, "Deadline already exceeded")
            return
        }

        while (!rateLimiter.tick()) {
            val remaining = deadline - System.currentTimeMillis()
            if (remaining <= 0) {
                reject(paymentId, transactionId, paymentStartedAt, "Deadline exceeded while waiting for rate limit")
                return
            }
            Thread.sleep(minOf(5L, remaining))
        }

        val timeRemaining = deadline - now
        val maxWaitTimeMs = (timeRemaining - expectedProcessingMs - 100).coerceAtLeast(100)

        if (maxWaitTimeMs < 100) {
            logger.warn("[$accountName] Payment $paymentId has insufficient time (${timeRemaining}ms) to wait for slot")
            reject(paymentId, transactionId, paymentStartedAt, "Insufficient time to wait for parallel slot")
            return
        }

        logger.debug("[$accountName] Payment $paymentId waiting for semaphore (max ${maxWaitTimeMs}ms, queue: ${parallelRequestsSemaphore.queueLength})")

        val acquired = try {
            parallelRequestsSemaphore.tryAcquire(maxWaitTimeMs, TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }

        if (!acquired) {
            val waitTime = System.currentTimeMillis() - now
            logger.warn("[$accountName] Payment $paymentId FAILED to acquire slot after ${waitTime}ms (queue length: ${parallelRequestsSemaphore.queueLength})")
            reject(paymentId, transactionId, paymentStartedAt, "Failed to acquire parallel slot within ${maxWaitTimeMs}ms")
            return
        }

        val afterWait = System.currentTimeMillis()
        val waitTime = afterWait - now

        if (afterWait >= deadline) {
            logger.warn("[$accountName] Payment $paymentId deadline exceeded while waiting ${waitTime}ms for slot")
            parallelRequestsSemaphore.release()
            reject(paymentId, transactionId, paymentStartedAt, "Deadline exceeded while waiting for slot")
            return
        }

        val timeForProcessing = deadline - afterWait
        if (timeForProcessing < expectedProcessingMs) {
            logger.warn("[$accountName] Payment $paymentId has only ${timeForProcessing}ms left, but need ${expectedProcessingMs}ms")
            parallelRequestsSemaphore.release()
            reject(paymentId, transactionId, paymentStartedAt, "Insufficient time for processing after wait")
            return
        }

        logger.info("[$accountName] Payment $paymentId acquired slot after ${waitTime}ms, ${timeForProcessing}ms remaining for processing")

        try {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, afterWait, Duration.ofMillis(waitTime))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val call = client.newCall(request)
            val httpTimeout = (timeForProcessing - 100).coerceAtLeast(1000)
            call.timeout().timeout(httpTimeout, TimeUnit.MILLISECONDS)

            call.execute().use { response ->
                val responseBody = response.body?.string()
                val body = try {
                    mapper.readValue(responseBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $responseBody")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, System.currentTimeMillis(), transactionId, reason = body.message)
                }
                recordProcessing(System.currentTimeMillis() - afterWait)
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(
                    false,
                    System.currentTimeMillis(),
                    transactionId,
                    reason = when (e) {
                        is SocketTimeoutException -> "HTTP request timeout after ${e.message}"
                        else -> e.message ?: "Unknown error"
                    }
                )
            }
        } finally {
            parallelRequestsSemaphore.release()
            logger.debug("[$accountName] Payment $paymentId released slot, queue length: ${parallelRequestsSemaphore.queueLength}")
        }
    }

    private fun reject(paymentId: UUID, transactionId: UUID, paymentStartedAt: Long, reason: String) {
        val now = System.currentTimeMillis()
        paymentESService.update(paymentId) {
            it.logSubmission(success = false, transactionId, now, Duration.ofMillis(now - paymentStartedAt))
            it.logProcessing(false, now, transactionId, reason = reason)
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private fun recordProcessing(durationMillis: Long) {
        processingSamples.addLast(durationMillis.coerceAtLeast(1))
        while (processingSamples.size > maxProcessingSamples) {
            processingSamples.pollFirst()
        }
    }

    private fun processingP99Millis(): Long {
        val values = processingSamples.toList()
        if (values.isEmpty()) {
            return expectedProcessingMs.coerceAtLeast(1)
        }
        val sorted = values.sorted()
        val index = ((sorted.size - 1) * 0.99).toInt()
        return sorted[index].coerceAtLeast(1)
    }
}