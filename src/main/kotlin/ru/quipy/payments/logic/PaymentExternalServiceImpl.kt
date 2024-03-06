package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: List<ExternalServiceProperties>,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val accountProcessingInfos = properties
        .map {
            it.toAccountProcessingInfo()
        }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newSingleThreadExecutor()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    private fun getAccountProcessingInfo(paymentStartedAt: Long): AccountProcessingInfo {
        while (true) {
            val curFirstCounter = accountProcessingInfos[0].requestCounter.get()
            val curSecondCounter = accountProcessingInfos[1].requestCounter.get()
            if (curSecondCounter >= accountProcessingInfos[1].parallelRequests
                || Duration.ofMillis(now() - paymentStartedAt) > accountProcessingInfos[1].request95thPercentileProcessingTime
            ) {
                if (curFirstCounter < accountProcessingInfos[0].parallelRequests) {
                    if (accountProcessingInfos[0].requestCounter.compareAndSet(curFirstCounter, curFirstCounter + 1)) {
                        return accountProcessingInfos[0]
                    }
                }
            } else {
                if (accountProcessingInfos[1].requestCounter.compareAndSet(curSecondCounter, curSecondCounter + 1)) {
                    return accountProcessingInfos[1]
                }
            }
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val accountProcessingInfo = getAccountProcessingInfo(paymentStartedAt)
        logger.warn("[${accountProcessingInfo.accountName}] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[${accountProcessingInfo.accountName}] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (Duration.ofMillis(now() - paymentStartedAt) > paymentOperationTimeout) {
            accountProcessingInfo.requestCounter.decrementAndGet()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${accountProcessingInfo.serviceName}&accountName=${accountProcessingInfo.accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[${accountProcessingInfo.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[${accountProcessingInfo.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error(
                        "[${accountProcessingInfo.accountName}] Payment failed for txId: $transactionId, payment: $paymentId",
                        e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            accountProcessingInfo.requestCounter.decrementAndGet()
        }
    }
}

fun ExternalServiceProperties.toAccountProcessingInfo(): AccountProcessingInfo = AccountProcessingInfo(this)

data class AccountProcessingInfo(
    private val properties: ExternalServiceProperties
) {
    val serviceName = properties.serviceName
    val accountName = properties.accountName
    val parallelRequests = properties.parallelRequests
    val rateLimitPerSec = properties.rateLimitPerSec
    val request95thPercentileProcessingTime = properties.request95thPercentileProcessingTime
    val requestCounter = AtomicInteger(0)
}

public fun now() = System.currentTimeMillis()