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
import java.util.concurrent.Semaphore
import kotlin.math.pow


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
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong() - 1, Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests)
    private val parallelRequestWaitingTimeMillis = 100L
    private var retryLimit = 3

    //private val rateLimiter = FixedWindowRateLimiter(rateLimitPerSec, 1, TimeUnit.SECONDS)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
//        while (!semaphore.tryAcquire())
//        {
//            Thread.sleep(parallelRequestWaitingTimeMillis);
//        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        rateLimiter.tickBlocking()
        if (isDeadlineWillExpired(deadline, requestAverageProcessingTime)) {
            paymentESService.update(paymentId) {
                it.logProcessing(
                    success = false,
                    now(),
                    transactionId,
                    reason = "Deadline will expired before request"
                )
            }
            return
        }
        semaphore.acquire()
        val retryManager = RetryManager(retryLimit, 10)
        do {
            try {

                if (isDeadlineWillExpired(deadline, requestAverageProcessingTime)) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            success = false,
                            now(),
                            transactionId,
                            reason = "Deadline will expired before request"
                        )
                    }
                    semaphore.release()
                    return
                }
                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, code: ${response.code}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    retryManager.setResponseStatus(body.result);
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
            }
        } while (retryManager.tryRetry())
        semaphore.release()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()

public fun isDeadlineWillExpired(deadlineTimeMillis: Long, requestAverageProcessingTime: Duration): Boolean  {
    return now() + requestAverageProcessingTime.toMillis() + requestAverageProcessingTime.toMillis()/4 >= deadlineTimeMillis
}

class RetryManager(
    private val retryLimit: Int,
    private val delayMillis: Long)
{
    private var factor: Double = 2.0
    private var retryCounter: Int = 0
    private var isSuccess: Boolean = false

    public fun tryRetry() : Boolean
    {
        if (!isSuccess && retryCounter < retryLimit)
        {
            val delayTime = (delayMillis * factor.pow(retryCounter.toDouble())).toLong()
            retryCounter++
            Thread.sleep(delayTime)
            return true
        }
        return false
    }

    public fun setResponseStatus(succeeded : Boolean )
    {
        isSuccess = succeeded
    }
}

