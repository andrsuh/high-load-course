package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.*
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val emptyBody = ByteArray(0).toRequestBody(null)
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    private val accountName = properties.accountName

    private val incomingRequestsCounter = Counter.builder("incoming.requests").tags("account", accountName).register(meterRegistry)
    private val incomingFinishedRequestsCounter = Counter.builder("incoming.finished.requests").tags("account", accountName).register(meterRegistry)
    private val outgoingRequestsCounter = Counter.builder("outgoing.requests").tags("account", accountName).register(meterRegistry)
    private val outgoingFinishedRequestsCounter = Counter.builder("outgoing.finished.requests").tags("account", accountName).register(meterRegistry)

    private val baseClient = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = 20000
            maxRequestsPerHost = 20000
        })
        .connectTimeout(60, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(60, TimeUnit.SECONDS)
        .build()

    private val slidingWindowLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ong = OngoingWindow(properties.parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        incomingRequestsCounter.increment()

        try {
            ong.acquire()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            return
        }

        if (!slidingWindowLimiter.tickBlocking(Duration.ofMillis(100))) {
            paymentESService.update(paymentId) {
                it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            ong.release()
            incomingFinishedRequestsCounter.increment()
            return
        }

        paymentESService.update(paymentId) {
            it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val remaining = deadline - System.currentTimeMillis()
        if (remaining <= 0) {
            ong.release()
            paymentESService.update(paymentId) { it.logProcessing(false, now(), transactionId, "Deadline exceeded") }
            incomingFinishedRequestsCounter.increment()
            return
        }

        val request = Request.Builder()
            .url("http://$paymentProviderHostPort/external/process?timeout=PT2S&serviceName=${properties.serviceName}&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()

        val clientWithTimeout = baseClient.newBuilder()
            .callTimeout(remaining.coerceAtMost(45_000), TimeUnit.MILLISECONDS)
            .build()

        outgoingRequestsCounter.increment()

        clientWithTimeout.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, "Technical error: ${e.message}")
                }
                ong.release()
                outgoingFinishedRequestsCounter.increment()
                incomingFinishedRequestsCounter.increment()
            }

            override fun onResponse(call: Call, response: Response) {
                val bodyBytes = response.body?.bytes() ?: byteArrayOf()

                val success = try {
                    val resp = mapper.readValue(bodyBytes, ExternalSysResponse::class.java)
                    paymentESService.update(paymentId) {
                        it.logProcessing(resp.result, now(), transactionId, resp.message)
                    }
                    resp.result
                } catch (e: Exception) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, "Parse error or HTTP ${response.code}")
                    }
                    false
                }

                ong.release()
                outgoingFinishedRequestsCounter.increment()
                incomingFinishedRequestsCounter.increment()
            }
        })
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()