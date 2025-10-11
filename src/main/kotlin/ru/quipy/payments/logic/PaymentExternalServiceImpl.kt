package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry
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

    private val incomingRequestsCounter: Counter = Counter
        .builder("incoming.requests")
        .description("Количество завершенных входящих запросов")
        .tags("account", properties.accountName)
        .register(meterRegistry)
    private val incomingFinishedRequestsCounter: Counter = Counter
        .builder("incoming.finished.requests")
        .description("Количество завершенных входящих запросов")
        .tags("account", properties.accountName)
        .register(meterRegistry)
    private val outgoingRequestsCounter: Counter = Counter
        .builder("outgoing.requests")
        .description("Количество исходящих запросов")
        .tags("account", properties.accountName)
        .register(meterRegistry)
    private val outgoingFinishedRequestsCounter: Counter = Counter
        .builder("outgoing.finished.requests")
        .description("Количество завершенных исходящих запросов")
        .tags("account", properties.accountName)
        .register(meterRegistry)

    private val client = OkHttpClient.Builder()
        .readTimeout(requestAverageProcessingTime.plusSeconds(10).toMillis(), TimeUnit.MILLISECONDS)
        .build()
    // Используем скользящее для "сглаживания" запросов к внешнему сервису по времени
    private val slidingWindowLimiter = SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))

    // Ограничиваем число одновременно выполняемых запросов (blocking window)
    private val ong = OngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        incomingRequestsCounter.increment()

        // сначала входим в окно (in-flight лимит)
        val remainingBeforeWindow = maxOf(0, deadline - System.currentTimeMillis())
        // ждать у слайдера будем недолго: не дольше остатка дедлайна и средней обработки
        val waitForSliderMs = minOf(remainingBeforeWindow, requestAverageProcessingTime.toMillis())
        ong.acquire()

        //коротко ждём у rate-лимитера, чтобы не держать слот окна слишком долго
        if (!slidingWindowLimiter.tickBlocking(Duration.ofMillis(waitForSliderMs))) {
            logger.warn("[$accountName] Payment $paymentId blocked by rate limiter after window")
            // submission как неотправленную
            paymentESService.update(paymentId) {
                it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            // и один раз фиксируем итог обработки
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "blocked by rate limiter after window")
            }
            ong.release()
            return
        }

        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        // Submission — только после прохождения обоих ворот (окно + слайдер)
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            outgoingRequestsCounter.increment()
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            //поджимаем вызов под дедлайн
            val remainingForCall = maxOf(0, deadline - System.currentTimeMillis())
            val perCallClient = client.newBuilder()
                .callTimeout(remainingForCall, TimeUnit.MILLISECONDS)
                .build()

            perCallClient.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
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
        finally {
            ong.release()
            incomingFinishedRequestsCounter.increment()
            outgoingFinishedRequestsCounter.increment()
            // освобождаем слот семафора
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()