package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
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

    private val capacityMultiplier = 1000
    private val requestQueue = PaymentRequestQueue(
        name = accountName,
        parallelRequests = parallelRequests,
        rateLimitPerSec = rateLimitPerSec,
        queueCapacity = rateLimitPerSec * capacityMultiplier,
        meterRegistry = meterRegistry,
        averageProcessingTime = requestAverageProcessingTime,
        onExecute = ::processPayment,
        onReject = ::rejectPayment,
    )

    private val incomingCounter = counter("incoming")
    private val outgoingCounter = counter("outgoing")
    private val rejectedEnqueueCounter = counter("rejected_enqueue")
    private val rejectedDispatchCounter = counter("rejected_dispatch")

    private fun counter(type: String) =
        meterRegistry.counter(
            "payment_sys_requests",
            "account", accountName,
            "type", type,
        )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        incomingCounter.increment()

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        requestQueue.submit(
            PaymentTask(
                paymentId = paymentId,
                amount = amount,
                paymentStartedAt = paymentStartedAt,
                deadline = deadline,
                transactionId = transactionId,
            )
        )
    }

    private fun rejectPayment(
        task: PaymentTask,
        reason: String,
        phase: PaymentRejectPhase,
    ) {
        when (phase) {
            PaymentRejectPhase.ENQUEUE ->
                rejectedEnqueueCounter.increment()

            PaymentRejectPhase.DISPATCH ->
                rejectedDispatchCounter.increment()
        }

        logger.warn(
            "[$accountName] Rejected payment ${task.paymentId}, " +
                    "txId: ${task.transactionId}, phase: $phase, " +
                    "reason: $reason"
        )

        paymentESService.update(task.paymentId) {
            it.logProcessing(
                false,
                now(),
                task.transactionId,
                reason = reason,
            )
        }
    }

    private fun recordProcessingResult(
        task: PaymentTask,
        success: Boolean,
        reason: String?,
    ) {
        try {
            paymentESService.update(task.paymentId) {
                it.logProcessing(
                    success,
                    now(),
                    task.transactionId,
                    reason = reason,
                )
            }
        } catch (e: Exception) {
            logger.error(
                "[$accountName] Failed to persist payment result: " +
                        "payment=${task.paymentId}, " +
                        "txId=${task.transactionId}, success=$success",
                e,
            )
        }
    }

    private fun processPayment(task: PaymentTask) {
        val paymentId = task.paymentId
        val transactionId = task.transactionId
        val amount = task.amount
        outgoingCounter.increment()

        val result: ExternalSysResponse = try {
            val request = Request.Builder()
                .url(
                    "http://$paymentProviderHostPort/external/process" +
                            "?serviceName=$serviceName" +
                            "&token=$token" +
                            "&accountName=$accountName" +
                            "&transactionId=$transactionId" +
                            "&paymentId=$paymentId" +
                            "&amount=$amount"
                )
                .post(emptyBody)
                .build()

            client.newCall(request).execute().use { response ->
                val responseText = response.body?.string()

                try {
                    mapper.readValue(
                        responseText,
                        ExternalSysResponse::class.java,
                    )
                } catch (e: Exception) {
                    logger.error(
                        "[$accountName] Invalid provider response: " +
                                "txId=$transactionId, " +
                                "HTTP ${response.code}, body=$responseText",
                        e,
                    )

                    ExternalSysResponse(
                        transactionId.toString(),
                        paymentId.toString(),
                        false,
                        e.message,
                    )
                }
            }
        } catch (e: Exception) {
            val reason = when (e) {
                is SocketTimeoutException -> "Request timeout."
                else -> e.message ?: "External payment request failed."
            }

            logger.error(
                "[$accountName] External payment failed: " +
                        "txId=$transactionId, payment=$paymentId",
                e,
            )

            ExternalSysResponse(
                transactionId.toString(),
                paymentId.toString(),
                false,
                reason,
            )
        }

        logger.info(
            "[$accountName] Payment processed: " +
                    "txId=$transactionId, " +
                    "payment=$paymentId, " +
                    "success=${result.result}, " +
                    "message=${result.message}"
        )

        recordProcessingResult(
            task = task,
            success = result.result,
            reason = result.message,
        )
    }

    override fun pendingRequests(): Int =
        requestQueue.pendingRequests()

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now(): Long = System.currentTimeMillis()