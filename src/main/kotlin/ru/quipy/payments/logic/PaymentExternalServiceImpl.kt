package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger


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

    private val waitDuration = Duration.ofSeconds(1)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newCachedThreadPool()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    private fun getAccountProcessingInfo(paymentStartedAt: Long): AccountProcessingInfo {
        while (true) {
            for (accountProcessingInfo in accountProcessingInfos) {
                val waitStartTime = now()
                while (Duration.ofMillis(now() - waitStartTime) <= waitDuration) {
                    val curRequestCount = accountProcessingInfo.requestCounter.get()
                    if (curRequestCount < accountProcessingInfo.parallelRequests &&
                        Duration.ofMillis(now() - paymentStartedAt) <=
                        Duration.ofMillis(paymentOperationTimeout.toMillis() - accountProcessingInfo.request95thPercentileProcessingTime.toMillis() * 2)) {
                        if (accountProcessingInfo.requestCounter.compareAndSet(curRequestCount, curRequestCount + 1)) {
                            return accountProcessingInfo
                        }
                    }
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

        if (Duration.ofMillis(now() - paymentStartedAt) > paymentOperationTimeout
            || accountProcessingInfo.requestCounter.get() > accountProcessingInfo.parallelRequests) {
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

        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
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

                accountProcessingInfo.requestCounter.decrementAndGet()
            }

            override fun onFailure(call: Call, e: IOException) {
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

                accountProcessingInfo.requestCounter.decrementAndGet()
            }
        })
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