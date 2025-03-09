package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

// Новый порог ожидания в очереди – 1000 мс.
const val MAX_QUEUE_WAIT_MS = 1000L
// Ограничение на максимальное число задач в очереди.
const val MAX_QUEUE_SIZE = 100

// Задача платежа с приоритетом. При равных дедлайнах используется время постановки.
data class PaymentTask(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val attempt: Int = 1,
    val enqueueTime: Long = now(),
    val transactionId: UUID = UUID.randomUUID()
) : Comparable<PaymentTask> {
    override fun compareTo(other: PaymentTask): Int {
        val cmp = this.deadline.compareTo(other.deadline)
        return if (cmp != 0) cmp else this.enqueueTime.compareTo(other.enqueueTime)
    }
}

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        const val MAX_RETRIES = 2
        const val INITIAL_BACKOFF_MS = 50L
        fun now() = System.currentTimeMillis()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // Ограничение по скорости – rateLimitPerSec запросов в секунду.
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    // Semaphore с fairness=true для соблюдения порядка.
    private val ongoingRequests = Semaphore(parallelRequests, true)
    private val client = OkHttpClient.Builder().build()
    private val scheduler = Executors.newSingleThreadScheduledExecutor()

    // Приоритетная очередь задач.
    private val pendingTasks = PriorityBlockingQueue<PaymentTask>()

    init {
        // Обработка очереди каждые 10 мс.
        scheduler.scheduleWithFixedDelay({ processPendingTasks() }, 0, 10, TimeUnit.MILLISECONDS)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        // Если очередь уже переполнена, отклоняем задачу.
        if (pendingTasks.size >= MAX_QUEUE_SIZE) {
            logger.error("[$accountName] Очередь переполнена, отклоняем задачу: paymentId=$paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), "Queue overload")
            }
            return
        }
        val task = PaymentTask(paymentId, amount, paymentStartedAt, deadline)
        pendingTasks.put(task)
        logger.info("[$accountName] Задача поставлена в очередь: paymentId=$paymentId, deadline=$deadline")
    }

    private fun processPendingTasks() {
        while (true) {
            val task = pendingTasks.peek() ?: break

            // Если задача уже просрочена, удаляем и логируем ошибку.
            if (now() > task.deadline) {
                pendingTasks.poll()
                logger.error("[$accountName] Дедлайн истёк в очереди: paymentId=${task.paymentId}")
                paymentESService.update(task.paymentId) {
                    it.logProcessing(false, now(), UUID.randomUUID(), "Deadline exceeded in queue")
                }
                continue
            }

            // Проверяем время ожидания задачи в очереди.
            val waitTime = now() - task.enqueueTime
            val semaphoreAcquired = if (waitTime >= MAX_QUEUE_WAIT_MS) {
                // Если задача ждёт слишком долго, пробуем сразу получить разрешение без ожидания.
                ongoingRequests.tryAcquire(0, TimeUnit.MILLISECONDS)
            } else {
                // Иначе ждем оставшееся время до дедлайна.
                val remainingTime = task.deadline - now()
                ongoingRequests.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)
            }

            if (!semaphoreAcquired) {
                // Если не получили разрешение, и задача уже слишком старая, удаляем её.
                if (waitTime >= MAX_QUEUE_WAIT_MS) {
                    pendingTasks.poll()
                    logger.error("[$accountName] Задача слишком долго ждёт, удаляем: paymentId=${task.paymentId}")
                    paymentESService.update(task.paymentId) {
                        it.logProcessing(false, now(), UUID.randomUUID(), "Exceeded max queue wait")
                    }
                }
                // Если разрешение не получено, выходим из цикла, чтобы не загружать систему.
                break
            }

            // Забираем задачу для обработки.
            val taskToProcess = pendingTasks.poll()
            if (taskToProcess == null) {
                ongoingRequests.release()
                break
            }

            processPaymentTask(taskToProcess)
        }
    }

    private fun processPaymentTask(task: PaymentTask) {
        // Пропускаем через rateLimiter.
        rateLimiter.tickBlocking()

        logger.info("[$accountName] Обработка задачи: paymentId=${task.paymentId}, txId=${task.transactionId}, attempt=${task.attempt}")

        paymentESService.update(task.paymentId) {
            it.logSubmission(true, task.transactionId, now(), Duration.ofMillis(now() - task.paymentStartedAt))
        }

        val requestUrl = "http://localhost:1234/external/process" +
                "?serviceName=$serviceName&accountName=$accountName&transactionId=${task.transactionId}" +
                "&paymentId=${task.paymentId}&amount=${task.amount}"
        val request = Request.Builder().url(requestUrl).post(emptyBody).build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                ongoingRequests.release()
                if (task.attempt < MAX_RETRIES && now() < task.deadline) {
                    logger.warn("[$accountName] Ошибка платежа: txId=${task.transactionId}, paymentId=${task.paymentId}, attempt=${task.attempt}, reason=${e.message}")
                    scheduleRetry(task, e.message ?: "Error")
                } else {
                    logger.error("[$accountName] Критическая ошибка: txId=${task.transactionId}, paymentId=${task.paymentId}", e)
                    paymentESService.update(task.paymentId) {
                        it.logProcessing(false, now(), task.transactionId, e.message ?: "Unknown error")
                    }
                }
            }

            override fun onResponse(call: Call, response: Response) {
                ongoingRequests.release()
                response.use { res ->
                    val externalResponse = try {
                        mapper.readValue(res.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Ошибка парсинга: txId=${task.transactionId}, paymentId=${task.paymentId}", e)
                        ExternalSysResponse(task.transactionId.toString(), task.paymentId.toString(), false, e.message)
                    }
                    if (!externalResponse.result && task.attempt < MAX_RETRIES && now() < task.deadline) {
                        logger.warn("[$accountName] Повтор платежа: txId=${task.transactionId}, paymentId=${task.paymentId}, reason=${externalResponse.message}")
                        scheduleRetry(task, externalResponse.message ?: "Negative response")
                    } else {
                        logger.info("[$accountName] Завершение платежа: txId=${task.transactionId}, success=${externalResponse.result}, reason=${externalResponse.message}")
                        paymentESService.update(task.paymentId) {
                            it.logProcessing(externalResponse.result, now(), task.transactionId, externalResponse.message)
                        }
                    }
                }
            }
        })
    }

    private fun scheduleRetry(task: PaymentTask, reason: String) {
        val newAttempt = task.attempt + 1
        val delayMillis = INITIAL_BACKOFF_MS * newAttempt
        if (now() + delayMillis > task.deadline) {
            logger.error("[$accountName] Недостаточно времени для повторной попытки: paymentId=${task.paymentId}")
            paymentESService.update(task.paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), "Deadline exceeded before retry")
            }
            return
        }
        // Создаём новую задачу с увеличенной попыткой.
        val newTask = task.copy(attempt = newAttempt, enqueueTime = now(), transactionId = UUID.randomUUID())
        scheduler.schedule({ pendingTasks.put(newTask) }, delayMillis, TimeUnit.MILLISECONDS)
        logger.info("[$accountName] Задача на повтор поставлена в очередь: paymentId=${task.paymentId}, новый attempt=$newAttempt, delay=$delayMillis ms, reason=$reason")
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
