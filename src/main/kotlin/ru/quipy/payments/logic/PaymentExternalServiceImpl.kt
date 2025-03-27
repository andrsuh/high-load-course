package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
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
import java.util.concurrent.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val resiliencePolicy: ResiliencePolicy,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    // Resilience policies
    private val timeoutPolicy = resiliencePolicy.timeout
    private val retryPolicy = resiliencePolicy.retry
    private val rateLimiterPolicy = resiliencePolicy.rateLimiter
    private val threadPoolPolicy = resiliencePolicy.threadPool

    // Account properties
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val averageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = rateLimiterPolicy.window
    )

    private val pool = ThreadPoolExecutor(
        parallelRequests, // corePoolSize
        parallelRequests * 2, // maximumPoolSize
        threadPoolPolicy.keepAliveTime.toMillis(), // keepAliveTime
        TimeUnit.MILLISECONDS, // time unit for keepAliveTime
        LinkedBlockingQueue(), // workQueue
        Executors.defaultThreadFactory(), // threadFactory
        ThreadPoolExecutor.AbortPolicy() // rejection handler
    )

    private val parallelRequestsSemaphore = Semaphore(parallelRequests)

    private val queueScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        pool.submit(Runnable {
            performPaymentCore(paymentId, amount, paymentStartedAt, deadline)
        })
    }

    private fun performPaymentCore(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        queueScope.launch {
            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Processing payment: $paymentId, txId: $transactionId")

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            var paymentUrl = "http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            if (timeoutPolicy.requestTimeout != null) {
                paymentUrl += "&timeout=${timeoutPolicy.requestTimeout}"
            }

            val request = Request.Builder().run {
                url(paymentUrl)
                post(emptyBody)
            }.build()

            if (!rateLimiter.tick()) {
                logger.warn("[$accountName] Payment $paymentId delayed due to rate limit")
                rateLimiter.tickBlocking()
            }

            withContext(Dispatchers.IO) {
                parallelRequestsSemaphore.acquire()
            }

            try {
                retry(
                    retryPolicy,
                    deadline,
                    shouldRetry = { response -> !response.result }
                ) {
                    executePaymentRequest(request, transactionId, paymentId)
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
                parallelRequestsSemaphore.release()
            }
        }
    }

    private fun executePaymentRequest(request: Request, transactionId: UUID, paymentId: UUID): ExternalSysResponse {
        client.newCall(request).execute().use { response ->
            val body = try {
                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
            }

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }

            return body
        }
    }

    private suspend fun <T> retry(
        policy: RetryPolicy,
        deadline: Long,
        shouldRetry: (T) -> Boolean,
        block: suspend () -> T
    ): T{
        return retry(
            maxAttempts = policy.maxAttempts,
            initialDelay = policy.initialDelay.toMillis(),
            maxDelay = policy.maxDelay.toMillis(),
            factor = policy.factor,
            deadline,
            shouldRetry,
            block
        )
    }

    private suspend fun <T> retry(
        maxAttempts: Int = 2,
        initialDelay: Long = 100,
        maxDelay: Long = 1000,
        factor: Double = 1.0,
        deadline: Long,
        shouldRetry: (T) -> Boolean,
        block: suspend () -> T
    ): T {
        var currentDelay = initialDelay

        repeat(maxAttempts - 1) { attempt ->
            val result = block()

            if (!shouldRetry(result))
                return result

            if (now() + currentDelay > deadline) {
                logger.warn("[$accountName] Deadline exceeded, stopping payment retries.")
                return result
            }

            logger.warn("[$accountName] Payment attempt ${attempt + 1} failed, retrying in $currentDelay ms...")

            delay(currentDelay)
            currentDelay = (currentDelay * factor).toLong().coerceAtMost(maxDelay)
        }

        return block() // last attempt
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()