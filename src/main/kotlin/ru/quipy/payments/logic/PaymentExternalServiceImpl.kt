package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import okhttp3.internal.http2.Http2
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min
import kotlin.random.Random

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val emptyBody = RequestBody.create(null, ByteArray(0))
        private val mapper = ObjectMapper().registerKotlinModule()

        private const val MAX_RETRIES = 5
        private const val INITIAL_RETRY_DELAY_MS = 500L
        private const val JITTER_MS = 300L
        private const val MAX_PARALLEL_REQUESTS = 6
        private const val FAILURE_THRESHOLD = 0.3
        private const val CALL_TIMEOUT_MS = 10_000L 
        private const val CACHE_SIZE = 100 
        private const val CONNECTION_POOL_SIZE = 10 
    }

    private val connectionPool = ConnectionPool(CONNECTION_POOL_SIZE, 5, TimeUnit.MINUTES)

    private val client = OkHttpClient.Builder()
        .connectTimeout(1, TimeUnit.SECONDS) 
        .readTimeout(3, TimeUnit.SECONDS)  
        .writeTimeout(1, TimeUnit.SECONDS)  
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .retryOnConnectionFailure(true)
        .build()

    private val executor = Executors.newFixedThreadPool(MAX_PARALLEL_REQUESTS)

    private val failedRequests = AtomicInteger(0)
    private val totalRequests = AtomicInteger(0)
    private val recentFailures = ConcurrentLinkedQueue<Long>()

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val cache = object : LinkedHashMap<UUID, Boolean>(CACHE_SIZE, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<UUID, Boolean>?): Boolean {
            return size > CACHE_SIZE
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.info("[$accountName] Старт платежа $paymentId")

        val cachedResult = cache[paymentId]
        if (cachedResult != null) {
            logger.info("[$accountName] Платеж $paymentId найден в кэше. Результат: $cachedResult")
            paymentESService.update(paymentId) {
                it.logProcessing(cachedResult, now(), paymentId, reason = "Cached result")
            }
            return
        }

        val transactionId = UUID.randomUUID()
        val request = buildRequest(paymentId, amount, transactionId, deadline)

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val retryCount = AtomicInteger(0)
        var isFastFailEnabled = false

        while (retryCount.get() < MAX_RETRIES && now() < deadline) {
            val remainingTime = deadline - now()
            if (remainingTime <= 0) {
                logger.error("[$accountName] Дедлайн истек, платеж $paymentId отменен.")
                break
            }

            val attempt = retryCount.incrementAndGet()

            if (!isFastFailEnabled && shouldEnableFastFail()) {
                logger.warn("[$accountName] Включен FAST-FAIL режим: слишком много отказов.")
                isFastFailEnabled = true
            }

            val future = executor.submit<Boolean> {
                executeWithTimeout(request, transactionId, paymentId, attempt, remainingTime, isFastFailEnabled)
            }

            try {
                if (future.get(remainingTime, TimeUnit.MILLISECONDS)) break
            } catch (e: TimeoutException) {
                logger.warn("[$accountName] Поток $attempt: превысил дедлайн.")
            } catch (e: Exception) {
                logger.error("[$accountName] Поток $attempt: ошибка выполнения", e)
            }
        }

        if (retryCount.get() >= MAX_RETRIES) {
            logger.error("[$accountName] Платеж $paymentId провален после $MAX_RETRIES попыток.")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Max retries exceeded.")
            }
        }

        logQueueAndThreadStats()
    }

    private fun buildRequest(paymentId: UUID, amount: Int, transactionId: UUID, deadline: Long): Request {
        return Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .addHeader("deadline", deadline.toString())
            .build()
    }

    private fun executeWithTimeout(request: Request, transactionId: UUID, paymentId: UUID, attempt: Int, remainingTime: Long, fastFail: Boolean): Boolean {
        val timeout = min(remainingTime, CALL_TIMEOUT_MS)
        val backoffTime = INITIAL_RETRY_DELAY_MS * (1 shl attempt) + Random.nextLong(JITTER_MS) 

        return try {
            val future = CompletableFuture<Boolean>()
            val startTime = now()

            val call = client.newCall(request)
            call.enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    logger.error("[$accountName] Поток $attempt: Ошибка запроса", e)
                    failedRequests.incrementAndGet()
                    recentFailures.add(now())
                    future.complete(false)
                }

                override fun onResponse(call: Call, response: Response) {
                    totalRequests.incrementAndGet()
                    try {
                        when (response.code) {
                            429 -> {
                                logger.warn("[$accountName] 429 Too Many Requests. Повторный запрос через backoff.")
                                Thread.sleep(backoffTime)
                                future.complete(false)
                            }
                            500, 502, 503, 504 -> {
                                logger.warn("[$accountName] Ошибка ${response.code}, отправляем в очередь на повтор.")
                                future.complete(false)
                            }
                            else -> {
                                val body = response.body?.string()?.let {
                                    try {
                                        mapper.readValue(it, ExternalSysResponse::class.java)
                                    } catch (e: Exception) {
                                        logger.error("[$accountName] Ошибка парсинга ответа: ${e.message}")
                                        null
                                    }
                                }
                                val result = body?.result ?: false
                                paymentESService.update(paymentId) {
                                    it.logProcessing(result, now(), transactionId, reason = body?.message)
                                }
                                cache[paymentId] = result
                                future.complete(result)
                            }
                        }
                    } finally {
                        response.close()
                    }
                }
            })

            val result = future.get(timeout, TimeUnit.MILLISECONDS)
            val duration = now() - startTime
            logRequestDuration(duration)
            result
        } catch (e: TimeoutException) {
            logger.error("[$accountName] Поток $attempt: Тайм-аут ответа.")
            false
        } catch (e: Exception) {
            logger.error("[$accountName] Поток $attempt: Ошибка", e)
            false
        }
    }

    private fun shouldEnableFastFail(): Boolean {
        val recentFailCount = recentFailures.count { it > now() - 10_000 }
        return recentFailCount > 3 && recentFailCount.toDouble() / (totalRequests.get() + 1) > FAILURE_THRESHOLD
    }

    private fun logRequestDuration(duration: Long) {
        logger.info("[$accountName] Запрос выполнен за $duration мс")
    }

    private fun logQueueAndThreadStats() {
        val executorService = executor as ThreadPoolExecutor
        val activeThreads = executorService.activeCount
        val queueSize = executorService.queue.size
        val completedTasks = executorService.completedTaskCount
        val totalTasks = executorService.taskCount

        logger.info("[$accountName] Статистика потоков: активные=$activeThreads, очередь=$queueSize, завершено=$completedTasks, всего=$totalTasks")
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
