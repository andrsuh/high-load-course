package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.min


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

    private val requestsQueue = PriorityBlockingQueue<PendingRequest>(parallelRequests)
    private val requestsInFlight = 0
    private val rateLimiter = LeakingBucketRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1), parallelRequests)

    private val coreExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(),
        NamedThreadFactory("payment-submission-executor")
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val pendingRequest = PendingRequest(paymentId, amount, paymentStartedAt, deadline)
        requestsQueue.add(pendingRequest)
    }

    override suspend fun startSubmitPayments() {
        rateLimiter.run()
    }

    override fun submitPayments() {
        for (i in 1..min(requestsQueue.size, rateLimitPerSec)) {
            val paymentRequest = requestsQueue.poll()

            logger.warn("[$accountName] Submitting payment request for payment ${paymentRequest.paymentId}")

            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submit for ${paymentRequest.paymentId} , txId: $transactionId")

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentRequest.paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentRequest.paymentStartedAt))
            }

            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=${paymentRequest.paymentId}&amount=${paymentRequest.amount}")
                post(emptyBody)
            }.build()

            coreExecutor.submit {
                try {
                    client.newCall(request).execute().use { response ->
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: ${paymentRequest.paymentId}, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentRequest.paymentId.toString(),false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: ${paymentRequest.paymentId}, succeeded: ${body.result}, message: ${body.message}")

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentRequest.paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    }
                } catch (e: Exception) {
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: ${paymentRequest.paymentId}", e)
                            paymentESService.update(paymentRequest.paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: ${paymentRequest.paymentId}", e)

                            paymentESService.update(paymentRequest.paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun canAcceptPayment(deadline: Long): Boolean {
        if (!rateLimiter.tick()) {
            return false
        }

        return now() + requestsQueue.size / rateLimitPerSec * 1000 + 1000 + requestAverageProcessingTime.toMillis() < deadline
    }

}

public fun now() = System.currentTimeMillis()