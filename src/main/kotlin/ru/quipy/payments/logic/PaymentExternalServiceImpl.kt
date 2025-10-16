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
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val emptyBody = RequestBody.create(null, ByteArray(0))
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val client = OkHttpClient.Builder().build()

    private val limiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofMillis(1000)
    )


    private val queue: BlockingQueue<PaymentTask> = LinkedBlockingQueue(1000)


    private val executor: ExecutorService = Executors.newFixedThreadPool(parallelRequests)

    init {
        repeat(parallelRequests) {
            executor.submit {
                while (true) {
                    try {
                        val task = queue.take()
                        limiter.tickBlocking()
                        processPayment(task)
                    } catch (e: InterruptedException) {
                        Thread.currentThread().interrupt()
                        break
                    } catch (e: Exception) {
                        logger.error("[$accountName] Unexpected error in worker", e)
                    }
                }
            }
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.info("[$accountName] Queueing payment request for payment $paymentId")

        val task = PaymentTask(paymentId, amount, paymentStartedAt, deadline)
        if (!queue.offer(task, 500, TimeUnit.MILLISECONDS)) {
            logger.error("[$accountName] Payment queue is full, dropping payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Queue overflow")
            }
        }
    }

    private fun processPayment(task: PaymentTask) {
        val (paymentId, amount, paymentStartedAt, _) = task
        val transactionId = UUID.randomUUID()

        logger.info("[$accountName] Submitting payment $paymentId (txId: $transactionId)")

        paymentESService.update(paymentId) {
            it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        try {
            val request = Request.Builder()
                .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                .post(emptyBody)
                .build()

            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}", e)
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            handleError(e, paymentId, transactionId)
        }
    }

    private fun handleError(e: Exception, paymentId: UUID, transactionId: UUID) {
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
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

data class PaymentTask(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long
)

fun now() = System.currentTimeMillis()
