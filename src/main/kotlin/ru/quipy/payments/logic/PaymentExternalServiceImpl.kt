package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.okhttp.MetricsInterceptor
import ru.quipy.common.utils.okhttp.RateLimiterInterceptor
import ru.quipy.common.utils.okhttp.WindowLimiterInterceptor
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metricsInterceptor: MetricsInterceptor,
    private val meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private const val CALL_TIMEOUT_IN_SECONDS: Long = 2
        private const val MAX_ATTEMPTS = 5
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val client = OkHttpClient
        .Builder()
        .addInterceptor(WindowLimiterInterceptor(parallelRequests))
        .addInterceptor(RateLimiterInterceptor(rateLimiter))
        .addInterceptor(metricsInterceptor)
        .callTimeout(Duration.ofSeconds(CALL_TIMEOUT_IN_SECONDS))
        .build()

    val retries = Counter.builder("account_payment_request_retries")
        .tag("account", accountName)
        .register(meterRegistry)

    private val executorScope = CoroutineScope(Dispatchers.IO)

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        executeRequestAsync(request, transactionId, paymentId, deadline)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private suspend fun executeRequestAsync(
        request: Request, transactionId: UUID, paymentId: UUID, deadline: Long, attempts: Int = 0
    ) {
        val deferredResponse = executorScope.async {
            client.newCall(request).execute()
        }

        try {
            val response = deferredResponse.await()
            processResponse(request, response, transactionId, paymentId, deadline, attempts + 1)
        } catch (e: Exception) {
            onFailure(request, e, transactionId, paymentId, deadline, attempts + 1)
        }
    }

    private suspend fun onFailure(
        request: Request, e: Exception, transactionId: UUID, paymentId: UUID, deadline: Long, attempt: Int
    ) {
        when (e) {
            is SocketTimeoutException, is InterruptedIOException -> {
                logger.error(
                    "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e
                )
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }

                if (attempt < MAX_ATTEMPTS && deadline - now() > requestAverageProcessingTime.toMillis()) {
                    retries.increment()
                    executeRequestAsync(request, transactionId, paymentId, deadline, attempt)
                }
            }

            else -> {
                logger.error(
                    "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e
                )
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
        }
    }

    private suspend fun processResponse(
        request: Request, response: Response, transactionId: UUID, paymentId: UUID, deadline: Long, attempt: Int
    ) {
        response.use {
            val body = try {
                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, message: ${body.message}, result code: ${response.code}")

            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(paymentId) {
                it.logProcessing(
                    body.result, now(), transactionId, reason = body.message
                )
            }

            if (!body.result && body.message == "Temporary error") {
                logger.warn("[$accountName] Temporary error, attempt ${attempt}/$MAX_ATTEMPTS")
                if (attempt < MAX_ATTEMPTS && deadline - now() > requestAverageProcessingTime.toMillis()) {
                    retries.increment()
                    executeRequestAsync(request, transactionId, paymentId, deadline, attempt)
                }
            }
        }
    }
}

fun now() = System.currentTimeMillis()
