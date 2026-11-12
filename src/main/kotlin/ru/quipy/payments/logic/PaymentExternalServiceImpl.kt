package ru.quipy.payments.logic

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tags
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.apigateway.ratelimit.PaymentCallAdmission
import java.io.IOException
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.UUID
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val attemptRecorder: PaymentAttemptRecorder,
    paymentProviderHostPort: String,
    private val token: String,
    private val paymentCallAdmission: PaymentCallAdmission,
    private val maxAttempts: Int = 5,
) : PaymentExternalSystemAdapter {
    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val emptyBody = ByteArray(0).toRequestBody(null)
        private val mapper = ObjectMapper().registerKotlinModule()
        private val retryableStatuses = setOf(429, 500, 502, 503, 504)
    }

    private val accountName = properties.accountName
    private val endpoint = "http://$paymentProviderHostPort/external/process".toHttpUrl()
    private val metricTags = Tags.of("account", accountName)
    private val deadlineDropped = Metrics.counter("payment_deadline_dropped", metricTags)

    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(false)
        .followRedirects(false)
        .addNetworkInterceptor { chain ->
            val response = chain.proceed(chain.request())
            // OkHttp retries 503 + Retry-After: 0 even with retryOnConnectionFailure(false).
            // Return this decision to our loop so the next attempt is logged and rate-limited.
            if (response.code == 503 && retryAfterMillis(response.header("Retry-After")) == 0L) {
                response.use { throw RetryableResponse(readOutcome(it)) }
            }
            response
        }
        .build()

    init {
        require(maxAttempts > 0) { "Payment max attempts must be positive" }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var retryDelayMs = 0L
        for (attempt in 1..maxAttempts) {
            val transactionId = UUID.randomUUID()
            // Every attempt has its own submission and processing event, including local rejection.
            attemptRecorder.submitted(paymentId, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))

            val outcome = when {
                !waitForRetry(retryDelayMs, deadline) -> deadlineOutcome("before attempt $attempt")
                !paymentCallAdmission.awaitCall(accountName, deadline) -> deadlineOutcome("waiting for account rate permit")
                else -> performAttempt(paymentId, transactionId, amount, deadline)
            }

            // A DB failure after a successful charge must not cause another charge.
            attemptRecorder.processed(paymentId, transactionId, outcome.success, now(), outcome.reason)
            logger.info("[{}] Payment {}, attempt {}, success: {}, reason: {}",
                accountName, paymentId, attempt, outcome.success, outcome.reason)

            if (outcome.success || !outcome.retryable) return
            retryDelayMs = outcome.retryDelayMs
        }
    }

    private fun performAttempt(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long): Outcome {
        val remainingMs = deadline - now()
        if (Thread.currentThread().isInterrupted || remainingMs <= 0) return deadlineOutcome("before external request")
        val request = Request.Builder()
            .url(endpoint.newBuilder()
                .addQueryParameter("serviceName", properties.serviceName)
                .addQueryParameter("token", token)
                .addQueryParameter("accountName", accountName)
                .addQueryParameter("transactionId", transactionId.toString())
                .addQueryParameter("paymentId", paymentId.toString())
                .addQueryParameter("amount", amount.toString())
                .addQueryParameter("timeout", Duration.ofMillis(remainingMs).toString())
                .build())
            .post(emptyBody)
            .build()
        val call = client.newCall(request)
        call.timeout().timeout(remainingMs, TimeUnit.MILLISECONDS)
        return try {
            call.execute().use(::readOutcome)
        } catch (exception: RetryableResponse) {
            exception.outcome
        } catch (exception: IOException) {
            // A lost response does not establish that the charge failed. Without a provider
            // idempotency contract, do not repeat a payment with an unknown outcome.
            Outcome(reason = "Unknown payment outcome: ${exception.javaClass.simpleName}")
        }
    }

    private fun readOutcome(response: Response): Outcome {
        val rawBody = response.body?.string()
        val body = rawBody?.let {
            try {
                mapper.readValue(it, ExternalSysResponse::class.java)
            } catch (_: JsonProcessingException) {
                null
            }
        }
        if (!response.isSuccessful) {
            return Outcome(
                retryable = response.code in retryableStatuses,
                reason = "HTTP ${response.code}: ${body?.message ?: response.message}",
                retryDelayMs = retryAfterMillis(response.header("Retry-After")) ?: 0,
            )
        }
        if (body == null) return Outcome(reason = "Invalid payment provider response")
        return Outcome(success = body.result, retryable = !body.result && body.message == "Temporary error", reason = body.message)
    }

    private fun retryAfterMillis(value: String?): Long? {
        if (value == null) return null
        val trimmed = value.trim()
        if (trimmed.matches(Regex("[0-9]+"))) {
            val seconds = trimmed.toLongOrNull() ?: return Long.MAX_VALUE
            return if (seconds > Long.MAX_VALUE / 1_000) Long.MAX_VALUE else seconds * 1_000
        }
        return try {
            (ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli() - now())
                .coerceAtLeast(0)
        } catch (_: DateTimeParseException) {
            null
        }
    }

    private fun waitForRetry(delayMs: Long, deadline: Long): Boolean {
        val availableMs = deadline - now() - properties.averageProcessingTime.toMillis()
        if (Thread.currentThread().isInterrupted || availableMs <= 0 || delayMs >= availableMs) return false
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                return false
            }
        }
        return !Thread.currentThread().isInterrupted && deadline - now() > properties.averageProcessingTime.toMillis()
    }

    private fun deadlineOutcome(reason: String): Outcome {
        deadlineDropped.increment()
        return Outcome(reason = "Deadline would be missed or payment cancelled: $reason")
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = accountName

    private data class Outcome(
        val success: Boolean = false,
        val retryable: Boolean = false,
        val reason: String? = null,
        val retryDelayMs: Long = 0,
    )
    private class RetryableResponse(val outcome: Outcome) : IOException("Retryable payment response")
}

fun now() = System.currentTimeMillis()
