package ru.quipy.payments.logic.PaymentStages

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.sync.Semaphore
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.ExternalSysResponse
import ru.quipy.payments.logic.PaymentAccountProperties
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl
import ru.quipy.payments.logic.PaymentStages.StageMarkers.ProcessMarker
import ru.quipy.payments.logic.PaymentStages.StageResults.ProcessResult
import ru.quipy.payments.logic.logProcessing
import ru.quipy.payments.logic.logSubmission
import ru.quipy.payments.logic.now
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*

class ProcessStage(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val properties: PaymentAccountProperties
) : PaymentStage<ProcessMarker, ProcessResult> {

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
    private val semaphore: Semaphore = Semaphore(parallelRequests)

    override suspend fun process(payment: Payment) : ProcessResult {
        logger.warn("[$accountName] Submitting payment request for payment ${payment.paymentId}")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for ${payment.paymentId} , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(payment.paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - payment.paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=${payment.paymentId}&amount=${payment.amount}")
            post(emptyBody)
        }.build()

        try {
            val response = client.newCall(request).execute()

            val body = try {
                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: ${payment.paymentId}, result code: ${response.code}, reason: ${response.body?.string()}")
                ExternalSysResponse(transactionId.toString(), payment.paymentId.toString(), false, e.message)
            }

            if (!body.result)
                return ProcessResult(retry = true)

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: ${payment.paymentId}, succeeded: ${body.result}, message: ${body.message}")

            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(payment.paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }

        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: ${payment.paymentId}", e)
                    paymentESService.update(payment.paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: ${payment.paymentId}", e)

                    paymentESService.update(payment.paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }

        return ProcessResult(retry = false)
    }
}