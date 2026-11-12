package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tags
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LatencyStats
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.SlidingWindowReservations
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.sqrt


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
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

    // Лимит провайдера - rateLimitPerSec за секунду, используем его полностью
    private val rateLimitWindow = Duration.ofSeconds(1)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), rateLimitWindow)

    // Не больше parallelRequests запросов одновременно в полёте; fair - чтобы ожидающие шли по FIFO и ранние оплаты не опоздали к дедлайну
    private val parallelLimiter = Semaphore(parallelRequests, true)

    private val providerRequests = Metrics.counter("shop_provider_requests", "account", accountName)

    // Сколько оплат в секунду реально пропускает аккаунт: упираемся либо в наш rate limiter, либо в окно parallelRequests
    private val rateBound = rateLimitPerSec * 1000.0 / rateLimitWindow.toMillis()
    private val windowBound = parallelRequests * 1000.0 / requestAverageProcessingTime.toMillis()
    private val throughputPerSec = minOf(rateBound, windowBound)

    // Сколько слот провайдера в среднем занят одним вызовом: от него зависит, как часто освобождаются места в окне parallelRequests
    private val slotHoldStats = LatencyStats()

    // Входной лимитер: бронирует каждой принятой оплате момент отправки к провайдеру.
    // Если упираемся в rate limit - то же скользящее окно, что и на выходе, но длиннее на шаг опроса выходного лимитера:
    // к моменту брони разрешение там уже точно освободилось, и запросы не накапливают отставание от расписания.
    // Этот же зазор - запас на разброс доставки, ведь провайдер считает лимит по времени прихода.
    // Если упираемся в окно - parallelRequests отправок за среднее время занятия слота (закон Литтла):
    // берём замер, а не обещанный avg, иначе расписание обгоняет реальность и опоздания копятся
    private val isWindowBound = windowBound < rateBound
    private val sendSchedule = if (isWindowBound) {
        SlidingWindowReservations(parallelRequests)
    } else {
        SlidingWindowReservations(rateLimitPerSec)
    }

    private fun scheduleWindow(horizon: Duration): Duration = if (isWindowBound) {
        Duration.ofMillis(slotHoldStats.meanOver(horizon)?.toLong() ?: requestAverageProcessingTime.toMillis())
    } else {
        rateLimitWindow + SlidingWindowRateLimiter.POLL_INTERVAL
    }

    // Сколько реально длится оплата у этого аккаунта (от забронированного момента отправки до ответа провайдера): меряем на лету
    private val executionStats = LatencyStats()

    // Текущая оценка худшего времени оплаты. Пока замеров нет, знаем только обещанное провайдером среднее avg
    private val worstExecutionMs = Metrics.gauge(
        "shop_payment_execution_max_ms", Tags.of("account", accountName), AtomicLong(requestAverageProcessingTime.toMillis())
    )!!

    // Худший замер за последнее время. Горизонт - сколько ждёт клиент (дедлайн из запроса): за это время
    // успевает пройти достаточно оплат, а старый выброс не влияет на решения дольше, чем живёт сам запрос
    private fun estimateWorstExecutionMs(horizon: Duration): Long {
        val worst = executionStats.maxOver(horizon) ?: requestAverageProcessingTime.toMillis()
        worstExecutionMs.set(worst)
        return worst
    }

    // Закон Литтла с верхней границей времени: оплата, которая длится дольше, чем клиент готов ждать, уже никому не нужна,
    // поэтому одновременно в полёте не больше throughputPerSec * (дедлайн - сейчас) оплат. Столько потоков пул никогда
    // не станет узким местом. Больше parallelRequests брать бессмысленно - лишние ждали бы на семафоре
    override fun parallelism(deadline: Long): Int {
        val inFlight = Math.ceil(throughputPerSec * (deadline - now()) / 1000.0).toInt()
        return inFlight.coerceIn(1, parallelRequests)
    }

    override fun reserveSendSlot(deadline: Long): Long? {
        val now = now()
        val horizon = Duration.ofMillis(deadline - now)
        val worst = estimateWorstExecutionMs(horizon)
        val holdStd = slotHoldStats.stdOver(horizon) ?: 0.0
        return sendSchedule.reserve(now, scheduleWindow(horizon)) { slot ->
            slot + waitJitter(slot - now, holdStd) + worst <= deadline
        }
    }

    // Если упираемся в окно, места освобождаются в случайные моменты (когда кто-то из parallelRequests закончил):
    // ожидание (впереди + 1) освобождений - сумма случайных времён, её разброс sigma * sqrt(впереди + 1) / parallelRequests,
    // где sigma - замеренный разброс времени занятия слота. Берём правило трёх сигм (99,7%).
    // Если упираемся в rate limit, разрешения выдаются строго по времени и разброса нет
    private fun waitJitter(waitMs: Long, holdStd: Double): Double {
        if (!isWindowBound) return 0.0
        val ahead = waitMs * throughputPerSec / 1000
        return 3 * holdStd * sqrt(ahead + 1) / parallelRequests
    }

    // Когда запрос с тем же запасом времени до дедлайна уже поместится: ближайший слот минус этот запас
    override fun retryAfter(deadline: Long): Long {
        val now = now()
        val budget = deadline - now - worstExecutionMs.get()
        return maxOf(now + (1000 / throughputPerSec).toLong(), sendSchedule.nextSlot(now, scheduleWindow(Duration.ofMillis(deadline - now))) - budget)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, sendAt: Long) {
        // Оплату засчитывает сам провайдер в момент ответа, поэтому меряем от забронированного момента до получения ответа:
        // сюда попадают и задержка старта задачи, и запись в базу перед отправкой, а запись результата после ответа - нет
        val startedAt = sendAt
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        // Слот провайдера держим только на время самого вызова: запись результата в базу идёт уже после
        parallelLimiter.acquire()
        val acquiredAt = now()
        val (code, rawBody) = try {
            rateLimiter.tickBlocking()

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            providerRequests.increment()
            client.newCall(request).execute().use { response -> response.code to response.body?.string() }
        } catch (e: Exception) {
            executionStats.record(now() - startedAt)
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = if (e is SocketTimeoutException) "Request timeout." else e.message)
            }
            return
        } finally {
            parallelLimiter.release()
            slotHoldStats.record(now() - acquiredAt)
        }
        executionStats.record(now() - startedAt)

        val body = try {
            mapper.readValue(rawBody, ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: $code, reason: $rawBody")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()