package ru.quipy.payments.logic

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tags
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import ru.quipy.apigateway.ratelimit.PaymentCallAdmission
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.ceil


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentCallAdmission: PaymentCallAdmission,
    private val maxAttempts: Int,
    private val retryableStatuses: Set<Int>,
    processingTimeFactor: Double,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val retryProcessingTime = Duration.ofMillis(ceil(requestAverageProcessingTime.toMillis() * processingTimeFactor).toLong())
    private val endpoint = "http://$paymentProviderHostPort/external/process".toHttpUrl()
    private val metricTags = Tags.of("account", accountName)
    private val deadlineDropped = Metrics.counter("payment_deadline_dropped", metricTags)

    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(false)
        .followRedirects(false)
        .addNetworkInterceptor { chain ->
            val response = chain.proceed(chain.request())
            if (response.code == HttpStatus.SERVICE_UNAVAILABLE.value() && retryAfterMillis(response.header("Retry-After")) == 0L) {
                response.use { throw RetryableResponse(readOutcome(it)) }
            }
            response
        }
        .build()

    init {
        require(maxAttempts > 0) { "Payment max attempts must be positive" }
        require(processingTimeFactor.isFinite() && processingTimeFactor > 0 && processingTimeFactor <= 1) {
            "Payment retry processing time factor must be finite, greater than zero and at most one"
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var retryDelayMs = 0L
        repeat(maxAttempts) { attempt ->
            val processingTimeEstimate = if (attempt == 0) requestAverageProcessingTime else retryProcessingTime
            val outcome = performAttempt(paymentId, amount, paymentStartedAt, deadline, retryDelayMs, processingTimeEstimate)
            if (outcome.success || !outcome.retryable) return
            retryDelayMs = outcome.retryDelayMs
        }
    }

    private fun performAttempt(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, retryDelayMs: Long, processingTimeEstimate: Duration): Outcome {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        if (!waitForRetry(retryDelayMs, deadline, processingTimeEstimate)) {
            dropByDeadline(paymentId, transactionId, "before external request")
            return Outcome()
        }

        if (!paymentCallAdmission.awaitCall(accountName, deadline, processingTimeEstimate)) {
            dropByDeadline(paymentId, transactionId, "waiting for account rate permit")
            return Outcome()
        }

        val remainingMs = remainingTimeMs(deadline)
        if (Thread.currentThread().isInterrupted || remainingMs <= 0) {
            dropByDeadline(paymentId, transactionId, "before external request")
            return Outcome()
        }

        val outcome = try {
            val request = Request.Builder()
                .url(endpoint.newBuilder()
                    .addQueryParameter("serviceName", serviceName)
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
            call.execute().use(::readOutcome)
        } catch (exception: RetryableResponse) {
            exception.outcome
        } catch (exception: IOException) {
            Outcome(reason = "Unknown payment outcome: ${exception.javaClass.simpleName}")
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${outcome.success}, message: ${outcome.reason}")

        paymentESService.update(paymentId) {
            it.logProcessing(outcome.success, now(), transactionId, reason = outcome.reason)
        }
        return outcome
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
            return TimeUnit.SECONDS.toMillis(seconds)
        }
        return try {
            (ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli() - now())
                .coerceAtLeast(0)
        } catch (_: DateTimeParseException) {
            null
        }
    }

    private fun remainingTimeMs(deadline: Long): Long = deadline - now()

    private fun waitForRetry(delayMs: Long, deadline: Long, processingTimeEstimate: Duration): Boolean {
        val availableMs = deadline - now() - processingTimeEstimate.toMillis()
        if (Thread.currentThread().isInterrupted || availableMs <= 0 || delayMs >= availableMs) return false
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                return false
            }
        }
        return !Thread.currentThread().isInterrupted && deadline - now() > processingTimeEstimate.toMillis()
    }

    private fun dropByDeadline(paymentId: UUID, transactionId: UUID, msg: String) {
        deadlineDropped.increment()
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Deadline would be missed: $msg")
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private data class Outcome(
        val success: Boolean = false,
        val retryable: Boolean = false,
        val reason: String? = null,
        val retryDelayMs: Long = 0,
    )
    private class RetryableResponse(val outcome: Outcome) : IOException("Retryable payment response")
}

fun now() = System.currentTimeMillis()
