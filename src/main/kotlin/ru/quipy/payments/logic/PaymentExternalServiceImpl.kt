package ru.quipy.payments.logic

import org.springframework.beans.factory.annotation.Autowired
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import ru.quipy.payments.api.PaymentMetric
import java.util.concurrent.TimeUnit
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private var metrics: PaymentMetric
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }
    
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private var rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private var rateLimiter = SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests, true)

    val inSemaphoreCounter = Gauge.builder(
            "availablePermits_in_semaphore",
            java.util.function.Supplier { semaphore.availablePermits().toDouble() }
        )
            .description("availablePermits in semaphore for account $accountName")
            .register(Metrics.globalRegistry)

    private val client = OkHttpClient.Builder().apply {
        if (properties.percentile90 != null) {
            callTimeout(properties.percentile90, TimeUnit.MILLISECONDS)
        }
    }.build()

    private val maxRetryAttempts = 4
    private val retryDelayMillis = 100L

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        if (checkDeadline(paymentId, transactionId, deadline)){
            return
        }

        metrics.semaphoreQueueCount.incrementAndGet()
        metrics.semaphoreQueueDurationTimer.record (Runnable{
            semaphore.acquire()
        } )
        metrics.semaphoreQueueCount.decrementAndGet()
        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            var send = false
            var n = 0
            var delay = 0L
            while (!send) {

                metrics.rateLimiterQueueCount.incrementAndGet()
                metrics.rateLimiterQueueDurationTimer.record (Runnable {
                    rateLimiter.tickBlocking()
                })
                metrics.rateLimiterQueueCount.decrementAndGet()     

                if (checkDeadline(paymentId, transactionId, deadline)){
                    return
                }

                metrics.incomingRequestsCounter.increment()

                try {
                    send = sendRequest(request, now(), transactionId, paymentId)

                    if (send) {
                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentId) {
                            it.logProcessing(send, now(), transactionId, reason = null)
                        }
                        break
                    }
                    n += 1
                    val retryResult = doRetry(n, "failed", delay, transactionId, paymentId, deadline)
                    if (!retryResult){
                        break
                    }
                    delay += retryDelayMillis
                }
                catch (e: java.io.InterruptedIOException) {
                    n += 1
                    val retryResult = doRetry(n, "timeout", delay, transactionId, paymentId, deadline)
                    if (!retryResult){
                        break
                    }
                    delay += retryDelayMillis
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.warn("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.warn("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
        finally {
            metrics.paymentResponceCounter.increment()
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun getProperties() : PaymentAccountProperties {
        return properties
    }

    override fun getNumberOfRequests() : Long {
        return rateLimiter.size()
    }

    override fun name() = properties.accountName

    fun checkDeadline(paymentId: UUID, transactionId: UUID, deadline: Long, delay: Long = 0)  : Boolean {
        if (deadline < (now()+properties.averageProcessingTime.toMillis() + delay)) {
            logger.error("goodby payment 2: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(success = false, now(), transactionId = transactionId, reason = "deadline")
            }
            return true
        }
        return false
    }

    fun sendRequest(request : Request, startCall: Long, transactionId: UUID, paymentId: UUID) : Boolean {
        var result = false
        client.newCall(request).execute().use { response ->
            val executionTimeMillis = System.currentTimeMillis() - startCall
            metrics.recordLatency(response.code, executionTimeMillis)
            val body = try {
                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
            }
            result = body.result
            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
        }
        return result
    }

    fun doRetry(n: Int, reason: String, delay: Long, transactionId: UUID, paymentId: UUID, deadline: Long) : Boolean{
        if (n >= maxRetryAttempts) {
            logger.warn("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, max retry for reason $reason attempts reached")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Max retry attempts reached")
            }
            return false
        }
        else {
            if (checkDeadline(paymentId, transactionId, deadline, delay)){
                logger.warn("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, client deadline will exceeded")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Client deadline exceeded")
                }
                return false
            }
            logger.warn("[$accountName] Payment retrying reason $reason for txId: $transactionId, payment: $paymentId, attempt: $n, delay: $delay ms")
            metrics.paymentRetryCounter.increment()
            Thread.sleep(delay)
            return true
        }
    }

}

public fun now() = System.currentTimeMillis()