package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

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
    //case-5
    //private val OK_HTTP_CLIENT_TIMEOUT = requestAverageProcessingTime.multipliedBy(2) //just because (excel)
    //parallelRequests / rateLimitPerSec.toLong()
    private val OK_HTTP_CLIENT_TIMEOUT = Duration.ofSeconds(1) //case-6 5win/5rps

    private val client = OkHttpClient.Builder()
        .callTimeout(OK_HTTP_CLIENT_TIMEOUT) //full call to serv, write + read
        .readTimeout(OK_HTTP_CLIENT_TIMEOUT) //only between reed packages
        .writeTimeout(OK_HTTP_CLIENT_TIMEOUT) //only between packages to write (send)
        .connectTimeout(OK_HTTP_CLIENT_TIMEOUT) //only for establishing connection
        .build()
    //case 1-2
        private val rpsLimiter = FixedWindowRateLimiter(rateLimitPerSec, 1, TimeUnit.SECONDS)
     private val parallelRequestSemaphore = Semaphore(parallelRequests)
     //case 3
//    private val rpsLimiter = LeakingBucketRateLimiter(
//        rateLimitPerSec,
//        Duration.ofSeconds(1),
//        rateLimitPerSec * 2
//    );
    //case 4
    private val statsService = StatisticService();
    private val requestCSVService = RequestCSVService();
    //private val rpsLimiter = CountingRateLimiter(rateLimitPerSec, 1, TimeUnit.SECONDS);


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        if (deadline - now() < statsService.getPercentile95()) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return
        }

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

             // (blocking) case-2, 5req on semaphore / 5 procTime = 1rps

        parallelRequestSemaphore.acquire()
        logger.info("Acquire. Semaphore queue length: ${parallelRequestSemaphore.queueLength}")
        rpsLimiter.tickBlocking()

        // case-3 практика показывает, что parallel совсем чуть-чуть ломается, если оставить только лимитер, без paralSemaphore
//        if (!rpsLimiter.tick()) {
//            logger.error("[$accountName] RPS for payment: $transactionId, payment: $paymentId")
//            paymentESService.update(paymentId) {
//                it.logProcessing(false, now(), transactionId, reason = "RPS reached")
//            }
//            parallelRequestSemaphore.release()
//            return
//        }

        try {
            val createdAt = now();
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                val callTime = now() - createdAt
                statsService.addTime(callTime)
                requestCSVService.addRequestData(callTime, response.code, paymentId, transactionId)

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                parallelRequestSemaphore.release()
                logger.info("Release. Semaphore queue length: ${parallelRequestSemaphore.queueLength}")
                if (!body.result) {
                    performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                }
            }
        } catch (e: Exception) {
            logger.info("Catch exception ${e.message}")
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
            parallelRequestSemaphore.release()
            logger.info("Release in catch. Semaphore queue length: ${parallelRequestSemaphore.queueLength}")
            performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()

