package ru.quipy.payments.dto

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import ru.quipy.common.utils.exceptions.ProcessingFailReason
import ru.quipy.common.utils.exceptions.RequestProcessingException
import ru.quipy.payments.logic.ExternalSysResponse
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.emptyBody
import java.io.IOException
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import ru.quipy.payments.logic.now
import java.util.concurrent.CompletableFuture

class PendingRequest(
    val transactionId: UUID,
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val maxRetries: AtomicLong,
    val deadline: Long,
    private val accountName: String,
    private val client: OkHttpClient,
) : Comparable<PendingRequest> {
    private val retriesAmount = AtomicLong(0)
    private val retryAfter = AtomicLong(0)

    private lateinit var url: String;
    private val createdAt = now()

    companion object {
        val mapper = ObjectMapper().registerKotlinModule()
    }

    override fun compareTo(other: PendingRequest): Int {
        return createdAt.compareTo(other.createdAt)
    }

    fun call(
        timeout: Long,
        responseHandler: (ExternalSysResponse) -> Unit,
        exceptionHandler: (PendingRequest, Throwable) -> Unit
    ): CompletableFuture<Unit> {
        if (retriesAmount.incrementAndGet() >= maxRetries.get()) {
            throw RequestProcessingException(ProcessingFailReason.TOO_MANY_RETRIES, "Too Many Retries")
        }

        val request = Request.Builder().run {
            url(url)
            post(emptyBody)
        }.build()

        val future = CompletableFuture<Unit>()

        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                try {
                    if (!response.isSuccessful) {
                        exceptionHandler(this@PendingRequest, handleFailedResponse(response))
                        future.completeExceptionally(handleFailedResponse(response))
                        return
                    }

                    val body = safeGetBody(response)
                    if (!body.result) {
                        val ex = RequestProcessingException(ProcessingFailReason.UNKNOWN, body.message)
                        exceptionHandler(this@PendingRequest, ex)
                        future.completeExceptionally(ex)
                        return
                    }

                    responseHandler(body)
                    future.complete(Unit) // Явно возвращаем Unit
                    response.close()
                } catch (e: Exception) {
                    future.completeExceptionally(e)
                } finally {
                    response.close() // Важно закрывать Response
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                exceptionHandler(this@PendingRequest,
                    RequestProcessingException(ProcessingFailReason.UNKNOWN, e.message))
                future.completeExceptionally(e)
            }
        })

        return future
    }

    fun setRetryAfter(delay: Long) {
        retryAfter.set(delay)
    }

    fun getRetries() = retriesAmount.get()

    private fun safeGetBody(response: Response): ExternalSysResponse {
        try {
            return mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
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

    fun setUrl(newUrl: String) {
        url = newUrl;
    }
}