// package ru.quipy.payments.logic
//
// import com.fasterxml.jackson.databind.ObjectMapper
// import com.fasterxml.jackson.module.kotlin.registerKotlinModule
// import kotlinx.coroutines.sync.Semaphore
// import okhttp3.OkHttpClient
// import okhttp3.Request
// import okhttp3.RequestBody
// import org.slf4j.LoggerFactory
// import ru.quipy.common.utils.CustomRateLimiter
// import ru.quipy.common.utils.OngoingWindow
// import ru.quipy.common.utils.SlidingWindowRateLimiter
// import ru.quipy.core.EventSourcingService
// import ru.quipy.payments.api.PaymentAggregate
// import java.lang.Long.min
// import java.net.SocketTimeoutException
// import java.time.Duration
// import java.util.*
//
//
// // Advice: always treat time as a Duration
// class PaymentExternalSystemAdapterImpl(
//     private val properties: PaymentAccountProperties,
//     private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
// ) : PaymentExternalSystemAdapter {
//
//     companion object {
//         val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
//
//         val emptyBody = RequestBody.create(null, ByteArray(0))
//         val mapper = ObjectMapper().registerKotlinModule()
//     }
//
//     private val serviceName = properties.serviceName
//     private val accountName = properties.accountName
//     private val requestAverageProcessingTime = properties.averageProcessingTime
//     private val rateLimitPerSec = properties.rateLimitPerSec
//     // private val parallelRequests = properties.parallelRequests
//     private val maxRetries = properties.maxRetries ?: 3 // Add default value if not provided
//     private val retryDelay = properties.retryDelay ?: Duration.ofMillis(500) // Add default value if not provided
//
//     private val parallelRequests = 30 // Увеличенное значение для параллельных запросов
//     private val client = OkHttpClient.Builder()
//         .readTimeout(10, TimeUnit.SECONDS)
//         .connectTimeout(5, TimeUnit.SECONDS)
//         .build()
//
//     private val rateLimiter = CustomRateLimiter(
//         min(rateLimitPerSec.toLong() * requestAverageProcessingTime.toMillis() / 1100, parallelRequests.toLong()),
//         requestAverageProcessingTime,
//     )
//
//     private val slidRate = SlidingWindowRateLimiter(
//         rate = rateLimitPerSec.toLong() - 2,
//         window = Duration.ofSeconds(1)
//     )
//
//     private val ongoingRate = OngoingWindow(
//         maxWinSize = parallelRequests
//     )
//
//     private val semaphore = Semaphore(parallelRequests)
//
//     // Дополнительный метод для повторных попыток (retry)
//     // private fun performPaymentWithRetry(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, retry: Int = 0) {
//     //     try {
//     //         performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
//     //     } catch (e: Exception) {
//     //         if (retry < 3) {  // Максимум 3 попытки
//     //             Thread.sleep((retry + 1) * 1000)  // Экспоненциальная задержка
//     //             performPaymentWithRetry(paymentId, amount, paymentStartedAt, deadline, retry + 1)
//     //         } else {
//     //             logger.error("$accountName Max retries exceeded for payment $paymentId", e)
//     //             paymentESService.update(paymentId) {
//     //                 it.logProcessing(false, now(), UUID.randomUUID(), reason = "Max retries exceeded")
//     //             }
//     //         }
//     //     }
//     // }
//
//     override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
//         logger.warn("[$accountName] Submitting payment request for payment $paymentId")
//
//         val transactionId = UUID.randomUUID()
//         logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")
//
//         // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
//         // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
//         paymentESService.update(paymentId) {
//             it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
//         }
//
//         semaphore.acquire()
//         val timeBeforeDeadline = deadline - now() - requestAverageProcessingTime.toMillis() * 2
//         if (timeBeforeDeadline <= 0) {
//             paymentESService.update(paymentId) {
//                 it.logProcessing(false, now(), transactionId, reason = "Request will cause timeout, stopped")
//             }
//             semaphore.release()
//             return
//         }
//
//         if (!rateLimiter.tickBlocking(deadline + requestAverageProcessingTime.toMillis())) {
//             paymentESService.update(paymentId) {
//                 it.logProcessing(false, now(), transactionId, reason = "Request will cause timeout, stopped")
//             }
//             semaphore.release()
//             return
//         }
//
//         try {
//             var attempt = 0
//             lateinit var body: ExternalSysResponse
//
//             do {
//                 attempt++
//
//                 val request = Request.Builder().run {
//                     url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
//                     post(emptyBody)
//                 }.build()
//
//                 try {
//                     client.newCall(request).execute().use { response ->
//                         body = try {
//                             mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
//                         } catch (e: Exception) {
//                             logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
//                             ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
//                         }
//
//                         logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
//
//                         // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
//                         // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
//                         paymentESService.update(paymentId) {
//                             it.logProcessing(body.result, now(), transactionId, reason = body.message)
//                         }
//
//                         // when (response.code) {
//                         //     400, 401, 403, 404, 405 -> {
//                         //         throw RuntimeException("Client error code: ${response.code}")
//                         //     }
//                         //     else -> { /* no op */ }
//                         // }
//                     }
//                 } catch (e: SocketTimeoutException) {
//                     logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
//                     paymentESService.update(paymentId) {
//                         it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
//                     }
//                     body = ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Request timeout.")
//                 } catch (e: Exception) {
//                     logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
//                     paymentESService.update(paymentId) {
//                         it.logProcessing(false, now(), transactionId, reason = e.message)
//                     }
//                     body = ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
//                 }
//
//                 if (!body.result && attempt < maxRetries) {
//                     logger.warn("[$accountName] Retrying payment request for txId: $transactionId, attempt $attempt/$maxRetries")
//                     Thread.sleep(retryDelay.toMillis())
//                 }
//             } while (!body.result && attempt < maxRetries)
//         } finally {
//             semaphore.release()
//         }
//     }
//
//     override fun price() = properties.price
//
//     override fun isEnabled() = properties.enabled
//
//     override fun name() = properties.accountName
// }
//
// public fun now() = System.currentTimeMillis()

package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.time.Duration.ofSeconds
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.abs
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
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
    // private val parallelRequests = properties.parallelRequests
    private val parallelRequests = 30 // Увеличенное значение для параллельных запросов

    private val requestQueue = ConcurrentLinkedQueue<PaymentRequest>()
    private val executor = Executors.newFixedThreadPool(parallelRequests)

    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000))

    // private val client = OkHttpClient.Builder().build()

    private val client = OkHttpClient.Builder()
        // .callTimeout((requestAverageProcessingTime.toMillis() * 1.1).toLong(), TimeUnit.MILLISECONDS)
        // .callTimeout(8, TimeUnit.SECONDS)
        // .readTimeout(10, TimeUnit.SECONDS)
        // .connectTimeout(5, TimeUnit.SECONDS)
        // .writeTimeout(5, TimeUnit.SECONDS)
        .build()

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Adding payment request for payment $paymentId to queue")
        requestQueue.add(PaymentRequest(paymentId, amount, paymentStartedAt, deadline))
        processQueue()
    }

    private fun processQueue() {
        while (requestQueue.isNotEmpty()) {
            val request = requestQueue.poll() ?: return
            executor.submit {
                runBlocking {
                    executePaymentWithTimeout(request)
                }
                // executePayment(request)
            }
        }
    }

    private suspend fun executePaymentWithTimeout(request: PaymentRequest) {
        try {
            withTimeout(Duration.ofSeconds(8).toMillis()) {
                executePayment(request)
            }
        } catch (e: TimeoutCancellationException) {
            logger.error("[$accountName] Request timed out for payment ${request.paymentId}", e)
            paymentESService.update(request.paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Request timeout.")
            }
        }
        // catch (e: TimeoutException) {
        //     logger.error("[$accountName] Request timed out for payment ${request.paymentId}", e)
        //     paymentESService.update(request.paymentId) {
        //         it.logProcessing(false, now(), UUID.randomUUID(), reason = "Request timeout.")
        //     }
        // }
    }

    private fun executePayment(request: PaymentRequest) {
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Processing payment for ${request.paymentId}, txId: $transactionId")

        paymentESService.update(request.paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - request.paymentStartedAt))
        }

        var attempt = 0
        val maxRetries = 5
        var delay = 1000L

        while (attempt < maxRetries) {
            limiter.tickBlocking()
            val httpRequest = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=${request.paymentId}&amount=${request.amount}")
                post(emptyBody)
            }.build()

            try {
                client.newCall(httpRequest).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: ${request.paymentId}, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), request.paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: ${request.paymentId}, succeeded: ${body.result}, message: ${body.message}")

                    if (body.result) {
                        paymentESService.update(request.paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        return
                    }

                    when (HttpStatus.valueOf(response.code)) {
                        HttpStatus.BAD_REQUEST,
                        HttpStatus.UNAUTHORIZED,
                        HttpStatus.FORBIDDEN,
                        HttpStatus.NOT_FOUND,
                        HttpStatus.METHOD_NOT_ALLOWED -> {
                            logger.error("[$accountName] Payment failed permanently for txId: $transactionId, code: ${response.code}")
                            return
                        }
                        HttpStatus.TOO_MANY_REQUESTS -> {
                            delay *= 2
                        }
                        // HttpStatus.TOO_MANY_REQUESTS,
                        HttpStatus.SERVICE_UNAVAILABLE -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, code: ${response.code}")
                            val retryAfter = response.headers["Retry-After"]?.toLongOrNull()
                            retryAfter?.let {
                                delay = retryAfter * 1000
                            }
                        }
                        HttpStatus.INTERNAL_SERVER_ERROR -> {
                            delay = 0
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, code: ${response.code}")
                        }
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: ${request.paymentId}", e)
                        paymentESService.update(request.paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: ${request.paymentId}", e)
                        paymentESService.update(request.paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                processQueue()
            }

            attempt++
            if (attempt < maxRetries) {
                val jitter = (delay * 0.3 * Math.random()).toLong()
                val finalDelay = min(delay + jitter, abs(request.deadline - now()))

                val adjustedDelay = finalDelay + jitter

                logger.info("Retrying in ${adjustedDelay}ms (attempt ${attempt + 1})")
                Thread.sleep(adjustedDelay)
            }
            // if (attempt < maxRetries) {
            //     val finalDelay = min(delay, abs(request.deadline - now()))
            //     logger.info("Retrying in ${finalDelay}ms (attempt ${attempt + 1})")
            //     Thread.sleep(finalDelay)
            // }
        }

        paymentESService.update(request.paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Max retries reached")
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

private data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long
)

public fun now() = System.currentTimeMillis()