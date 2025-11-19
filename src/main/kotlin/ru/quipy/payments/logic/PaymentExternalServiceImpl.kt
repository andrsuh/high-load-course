package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    @Autowired private val metricsReporter: MetricsReporter
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val client = OkHttpClient.Builder().callTimeout((requestAverageProcessingTime.toMillis() * 2), TimeUnit.MILLISECONDS).build()
    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val maxRetryCount = 3
    private val maxDelay = 1000L
    private val startDelay = 200L

    private fun calculateBackOff(attempt: Int): Long {

        if (attempt <= 0) {
            return startDelay;
        }

        var factor = 1;

        repeat(attempt - 1) {
            factor *= 2;
        }

        val delay = startDelay * factor;

        if (delay > maxDelay) {
            return  maxDelay
        }

        return delay;
    }

    private fun timeOutOrGetAccessByRateLimiter(deadline: Long): Boolean {

        val minSleepMillis = (1000L / rateLimitPerSec.coerceAtLeast(1)); // один квант времени между запросами ~~ 1 / rateLimitPerSec сек

        while (true) {

            val nowMillis = now();

            if (nowMillis >= deadline) {
                return false;
            }

            if (rateLimiter.tick()) {
                return true;
            }

            val remaining = deadline - nowMillis;

            val sleepMillis = minOf(minSleepMillis, remaining);

            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {

        logger.warn("[$accountName] Submitting payment request for payment $paymentId");
        val transactionId = UUID.randomUUID();

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt));
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId");

        ongoingWindow.acquire();

        if (!timeOutOrGetAccessByRateLimiter(deadline)) {

            logger.error("[$accountName] Rate limit wait exceeded deadline for txId: $transactionId, payment: $paymentId");

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit wait exceeded deadline.");
            }

            return;
        }

        try {
            var amountOfRetries = 0;

            val toBlock = deadline - System.currentTimeMillis()

            while (toBlock >=0 && amountOfRetries < maxRetryCount) {
                try {
                    ++amountOfRetries;

                    val request = Request.Builder().run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                    }.build();

                    client.newCall(request).execute().use { response ->
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        }catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                        }

                        ongoingWindow.release()

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message);
                        }

                        if (body.result || (amountOfRetries == maxRetryCount)) {
                            break;
                        }

                        Thread.sleep(calculateBackOff(amountOfRetries));
                    }
                }

                catch (e: java.io.InterruptedIOException) {

                    logger.warn("[$accountName] Request interrupted by client timeout for txId=$transactionId (attempt $amountOfRetries/$maxRetryCount)")

                    if (amountOfRetries < maxRetryCount && now() < deadline) {
                        Thread.sleep(calculateBackOff(amountOfRetries))
                        continue
                    }
                    else {

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Client timeout after $maxRetryCount retries.")
                        }
                    }
                }
            }

        }
        catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()