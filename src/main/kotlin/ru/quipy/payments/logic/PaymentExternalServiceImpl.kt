package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import ru.quipy.common.utils.NonBlockingSlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()

        val CONNECT_TIMEOUT: Duration = Duration.ofMillis(3000)
        const val MAX_RETRY_ATTEMPTS = 3
        const val MAX_CONNECTIONS = 25000
        const val PENDING_ACQUIRE_TIMEOUT_SEC = 60L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = NonBlockingSlidingWindowRateLimiter(
        rate = rateLimitPerSec,
        window = Duration.ofSeconds(1)
    )

    private val paymentScope = CoroutineScope(
        Dispatchers.IO + SupervisorJob() + CoroutineName("payment-service-$accountName")
    )

    private val connectionProvider = ConnectionProvider.builder("payment-service-$accountName")
        .maxConnections(MAX_CONNECTIONS)
        .pendingAcquireTimeout(Duration.ofSeconds(PENDING_ACQUIRE_TIMEOUT_SEC))
        .pendingAcquireMaxCount(-1)
        .maxIdleTime(Duration.ofSeconds(30))
        .build()

    private val webClient: WebClient by lazy {
        val timeoutMs = (requestAverageProcessingTime.toMillis() * 1.5).toLong()

        val httpClient = HttpClient.create(connectionProvider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT.toMillis().toInt())
            .responseTimeout(Duration.ofMillis(timeoutMs))
            .doOnConnected { conn ->
                conn.addHandlerLast(ReadTimeoutHandler(timeoutMs, TimeUnit.MILLISECONDS))
                conn.addHandlerLast(WriteTimeoutHandler(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
            }

        WebClient.builder()
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .build()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        paymentScope.launch {
            try {
                executePaymentSuspend(paymentId, amount, paymentStartedAt, deadline)
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to process payment $paymentId", e)
            }
        }
    }

    private suspend fun executePaymentSuspend(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        withContext(Dispatchers.IO) {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var attempt = 0
        var message: String? = null
        var success = false

        while (!success && attempt++ < MAX_RETRY_ATTEMPTS) {
            try {
                val timeUntilDeadline = deadline - now()
                if (timeUntilDeadline <= 0) {
                    message = "Deadline exceeded"
                    break
                }

                if (!rateLimiter.acquireSuspendWithTimeout(timeUntilDeadline)) {
                    message = "Deadline exceeded"
                    break
                }

                webClient.post()
                    .uri("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    .awaitExchange { response ->
                        val statusCode = response.statusCode()
                        if (statusCode.is2xxSuccessful) {
                            val body = try {
                                val body = response.awaitBody<String>()
                                mapper.readValue(body, ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Failed to parse response for txId: $transactionId, payment: $paymentId", e)
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                            message = body.message
                            success = body.result
                        }
                    }
            } catch (e: Exception) {
                if (!isRetryableException(e)) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    message = e.message
                    break
                }
            }
        }

        withContext(Dispatchers.IO) {
            paymentESService.update(paymentId) {
                it.logProcessing(success, now(), transactionId, reason = message)
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

fun isRetryableException(e: Exception): Boolean {
    if (e is WebClientResponseException) {
        return e.statusCode.value() == 429 || e.statusCode.is5xxServerError
    }
    return e is SocketTimeoutException ||
            e is IOException || e.cause?.let { isRetryableException(it as? Exception ?: return@let false) } ?: false
}

public fun now() = System.currentTimeMillis()