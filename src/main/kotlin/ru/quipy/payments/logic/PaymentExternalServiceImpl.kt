package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import okhttp3.*
import org.HdrHistogram.Histogram
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

class DynamicTimeoutInterceptor(private val getTimeout: () -> Long) : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
        val timeout = getTimeout().toInt()
        val newChain = chain
            .withConnectTimeout(timeout, TimeUnit.MILLISECONDS)
            .withReadTimeout(timeout, TimeUnit.MILLISECONDS)
            .withWriteTimeout(timeout, TimeUnit.MILLISECONDS)
        return newChain.proceed(chain.request())
    }
}

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private sealed class Result<out T> {
        data class Success<out T>(val value: T) : Result<T>()
        object Retry : Result<Nothing>()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val retryCount = 3

    private var currentTimeout90thPercentile: Duration = Duration.ofMillis(requestAverageProcessingTime.toMillis() * 5)
    private var highestTrackableValue = requestAverageProcessingTime.toMillis() * 8
    private val histogram = Histogram(requestAverageProcessingTime.toMillis(), highestTrackableValue, 2)

    @OptIn(ExperimentalCoroutinesApi::class)
    private val scope = CoroutineScope(Dispatchers.IO.limitedParallelism(parallelRequests) + SupervisorJob())

    private val client = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(18, 5, TimeUnit.MINUTES))
        .addInterceptor(DynamicTimeoutInterceptor { currentTimeout90thPercentile.toMillis() })
        .build()

    private val rateLimiterBucket = TokenBucketRateLimiter(
        rateLimitPerSec,
        window = requestAverageProcessingTime.toMillis(),
        bucketMaxCapacity = rateLimitPerSec,
        timeUnit = TimeUnit.MILLISECONDS
    )

    private val semaphore = Semaphore(parallelRequests)

    private val baseDelay = 200L
    private val maxDelay = 1000L

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        scope.launch {
            logger.info("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val url = "http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&" +
                    "transactionId=$transactionId&paymentId=$paymentId&amount=$amount&$currentTimeout90thPercentile"
            val request = Request.Builder()
                .url(url)
                .post(emptyBody)
                .build()

            val startTime = System.currentTimeMillis()

            try {
                semaphore.acquire()

                var attempt = 1
                var isSuccess = false
                while (attempt <= retryCount && !isDeadlineExceeded(deadline)) {
                    rateLimiterBucket.tick()

                    val result = withTimeoutOrNull(currentTimeout90thPercentile.toMillis()) {
                        executeRequest(request, paymentId, transactionId)
                    }

                    when (result) {
                        is Result.Success -> {
                            isSuccess = result.value
                            if (isSuccess) break
                        }
                        is Result.Retry -> {
                            val delay = calculateRetryDelay(attempt, deadline)
                            if (delay > 0) {
                                delay(delay)
                            } else {
                                break
                            }
                        }
                        null -> break
                    }
                    attempt++
                }

                if (!isSuccess) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Max retries exceeded.")
                    }
                }
            } catch (e: Exception) {
                handleException(e, paymentId, transactionId)
            } finally {
                recordMetrics(startTime)
                semaphore.release()
            }
        }
    }

    private fun executeRequest(request: Request, paymentId: UUID, transactionId: UUID): Result<Boolean> {
        return try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] Error parsing response for payment $paymentId", e)
                    return Result.Retry
                }

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                when {
                    body.result -> Result.Success(true)
                    response.code == 429 -> {
                        handleRateLimit(response)
                        Result.Retry
                    }
                    response.code in 500..599 -> Result.Retry
                    else -> Result.Success(false)
                }
            }
        } catch (e: Exception) {
            if (e is SocketTimeoutException) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                Result.Success(false)
            } else {
                Result.Retry
            }
        }
    }

    private fun handleRateLimit(response: Response) {
        val retryAfter = response.header("Retry-After")?.toLongOrNull()?.times(1000)
            ?: baseDelay
        logger.warn("[$accountName] Rate limited, retry after $retryAfter ms")
    }

    private fun isDeadlineExceeded(deadline: Long): Boolean {
        return now() > deadline - currentTimeout90thPercentile.toMillis()
    }

    private fun calculateRetryDelay(attempt: Int, deadline: Long): Long {
        val delay = minOf(baseDelay * (1L shl (attempt - 1)), maxDelay)
        return if (now() + delay < deadline) delay else -1
    }

    private fun handleException(e: Exception, paymentId: UUID, transactionId: UUID) {
        val reason = when (e) {
            is SocketTimeoutException -> "Request timeout"
            else -> e.message ?: "Unknown error"
        }
        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    private fun recordMetrics(startTime: Long) {
        val duration = System.currentTimeMillis() - startTime
        histogram.recordValue(duration)
        currentTimeout90thPercentile = Duration.ofMillis(
            minOf(histogram.getValueAtPercentile(90.0), highestTrackableValue)
        )
    }



    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}
fun now() = System.currentTimeMillis()