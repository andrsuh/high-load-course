package ru.quipy.payments.dto

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.*
import org.eclipse.jetty.http.HttpHeader
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.exceptions.ProcessingFailReason
import ru.quipy.common.utils.exceptions.RequestProcessingException
import ru.quipy.payments.logic.ExternalSysResponse
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.emptyBody
import java.io.IOException
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class PendingRequest(
    val transactionId: UUID,
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val maxRetries: AtomicLong,
    val deadline: Long,
    private val accountName: String,
) : Comparable<PendingRequest> {
    private val retriesAmount = AtomicLong(0)
    private val retryAfter = AtomicLong(0)

    companion object {
        val logger = LoggerFactory.getLogger(PendingRequest::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val client = OkHttpClient
        .Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(40, TimeUnit.SECONDS)
        .writeTimeout(2, TimeUnit.SECONDS)
        .connectionPool(ConnectionPool(
            maxIdleConnections = 50,  // Макс. количество бездействующих соединений
            keepAliveDuration = 5,    // Время жизни соединения (мин)
            timeUnit = TimeUnit.MINUTES
        ))
        .dispatcher(Dispatcher().apply {
            maxRequests = 200         // Общее макс. количество запросов
            maxRequestsPerHost = 50   // Макс. запросов на один хост
        })
        .build()

    override fun compareTo(other: PendingRequest): Int {
        if (retryAfter.get() != 0L && other.retryAfter.get() != 0L) {
            return retryAfter.get().compareTo(other.retryAfter.get())
        }

        if (retryAfter.get() != 0L) {
            return retryAfter.get().toInt()
        }

        return deadline.compareTo(other.deadline)
    }

    fun call(url: String, timeout: Long, responseHandler: (ExternalSysResponse) -> Unit, exceptionHandler: (PendingRequest, Throwable) -> Unit) {
        if (retriesAmount.incrementAndGet() >= maxRetries.get()) {
            throw RequestProcessingException(ProcessingFailReason.TOO_MANY_RETRIES, "Too Many Retries")
        }

        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val request = Request.Builder().run {
            url(url)
            header(HttpHeader.KEEP_ALIVE.toString(), "timeout=${timeout / 1000}")
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                exceptionHandler(this@PendingRequest, RequestProcessingException(ProcessingFailReason.UNKNOWN, e.message))
            }

            override fun onResponse(call: Call, response: Response) {
                try {
                    if (!response.isSuccessful) {
                        exceptionHandler(this@PendingRequest, handleFailedResponse(response))

                        return
                    }

                    val body = safeGetBody(response)

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    if (!body.result) {
                        // успешный ответ с ошибкой
                        exceptionHandler(this@PendingRequest, RequestProcessingException(ProcessingFailReason.UNKNOWN, body.message))

                        return
                    }

                    responseHandler(body)
                } finally {
                    response.close()
                }
            }
        })
    }

    fun setRetryAfter(delay: Long) {
        retryAfter.set(delay)
    }

    fun getRetries() = retriesAmount.get()

    private fun safeGetBody(response: Response): ExternalSysResponse {
        try {
            return mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${e.message}")

            return ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }
    }

    private fun handleFailedResponse(response: Response): RequestProcessingException {
        val body = safeGetBody(response)

        return when (response.code) {
            429 -> RequestProcessingException(ProcessingFailReason.TOO_MANY_REQUESTS, body.message)
            500 -> RequestProcessingException(ProcessingFailReason.INTERNAL_SERVER_ERROR, body.message)
            502 -> RequestProcessingException(ProcessingFailReason.BAD_GATEWAY, body.message)
            503 -> RequestProcessingException(ProcessingFailReason.SERVICE_UNAVAILABLE, body.message)
            504 -> RequestProcessingException(ProcessingFailReason.GATEWAY_TIMEOUT, body.message)
            408 -> RequestProcessingException(ProcessingFailReason.TIMEOUT, body.message)

            400 -> RequestProcessingException(ProcessingFailReason.BAD_REQUEST, body.message)
            401 -> RequestProcessingException(ProcessingFailReason.UNAUTHORIZED, body.message)
            403 -> RequestProcessingException(ProcessingFailReason.FORBIDDEN, body.message)
            404 -> RequestProcessingException(ProcessingFailReason.NOT_FOUND, body.message)
            405 -> RequestProcessingException(ProcessingFailReason.METHOD_NOT_ALLOWED, body.message)

            else -> RequestProcessingException(ProcessingFailReason.UNKNOWN)
        }
    }
}