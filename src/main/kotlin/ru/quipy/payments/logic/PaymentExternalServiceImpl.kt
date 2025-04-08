package ru.quipy.payments.logic

import kotlinx.coroutines.*
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import java.util.concurrent.Semaphore
import okhttp3.RequestBody.Companion.toRequestBody
import ru.quipy.common.utils.*
import ru.quipy.common.utils.exceptions.RETRIABLE_PROCESSING_FAIL_REASONS
import ru.quipy.common.utils.exceptions.RequestProcessingException
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.dto.InFlightRequest
import ru.quipy.payments.dto.PendingRequest
import java.net.SocketTimeoutException
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min
import com.google.common.util.concurrent.RateLimiter


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {
    private val scheduledExecutorScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    companion object {
        val emptyBody = ByteArray(0).toRequestBody(contentType = null)
    }

    private val outgoingRateLimiter = RateLimiter.create(1100.0)
    private val inFlightRequestsLimiter = Semaphore(20000)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestsQueue = LinkedBlockingDeque<PendingRequest>(parallelRequests)

    private val coreExecutor = ThreadPoolExecutor(
        512,
        1024,
        30L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(200000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val client = OkHttpClient
        .Builder()
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(30, TimeUnit.SECONDS)
        .connectionPool(
            ConnectionPool(
                maxIdleConnections = 2000,  // Макс. количество бездействующих соединений
                keepAliveDuration = 1,    // Время жизни соединения
                timeUnit = TimeUnit.MINUTES
            )
        )
        .pingInterval(100, TimeUnit.MILLISECONDS)
        .dispatcher(Dispatcher().apply {
            maxRequests = 20000
            maxRequestsPerHost = 20000
        })
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        val pendingRequest =
            PendingRequest(
                transactionId,
                paymentId,
                amount,
                paymentStartedAt,
                getMaxRetries(amount, deadline),
                deadline,
                accountName,
                client
            )

        val url = buildUrl(pendingRequest)
        pendingRequest.setUrl(url)

        requestsQueue.add(pendingRequest)
    }

    override fun submitPayments() {
        val batchSize = min(100, inFlightRequestsLimiter.availablePermits())
        val requests = mutableListOf<PendingRequest>()

        requestsQueue.drainTo(requests, batchSize)

        if (requests.isEmpty()) return

        if (!outgoingRateLimiter.tryAcquire(requests.size)) {
            requestsQueue.addAll(requests)
            return
        }

        inFlightRequestsLimiter.acquire(requests.size)

        requests.forEach { request ->
            CompletableFuture.runAsync({
                InFlightRequest(
                    request, paymentESService,
                    2 * requestAverageProcessingTime.toMillis(), ::handleException
                ).run().whenComplete { _, _ -> inFlightRequestsLimiter.release() }

            }, coreExecutor)
        }
    }

    private fun buildUrl(request: PendingRequest): String {
        val urlBuilder = StringBuilder(256)

        return urlBuilder.apply {
            append("http://localhost:1234/external/process?")
            append("serviceName=$serviceName&")
            append("accountName=$accountName&")
            append("transactionId=${request.transactionId}&")
            append("paymentId=${request.paymentId}&")
            append("amount=${request.amount}&")
            append("timeout=PT${"%.3f".format(2 * requestAverageProcessingTime.toMillis() / 1000.00)}s")
        }.toString()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun canAcceptPayment(deadline: Long): Boolean {
        if (deadline.toDouble() < now().toDouble() + (requestsQueue.size + 1).toDouble() / selectMaxRps() * 1000 + requestAverageProcessingTime.toMillis()) {
            throw IllegalStateException("Time limits for $accountName breached")
        }

        return true
    }

    private fun handleException(paymentRequest: PendingRequest, e: Throwable) {
        when (e) {
            is SocketTimeoutException -> {
                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), paymentRequest.transactionId, reason = "Request timeout.")
                }
            }

            is RequestProcessingException -> {
                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), paymentRequest.transactionId, reason = e.message)
                }

                if (RETRIABLE_PROCESSING_FAIL_REASONS.contains(e.failReason)) {
                    paymentRequest.setRetryAfter((paymentRequest.deadline - now()) / (paymentRequest.maxRetries.get() - paymentRequest.getRetries()))
                    requestsQueue.add(paymentRequest)
                }
            }

            else -> {
                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), paymentRequest.transactionId, reason = e.message)
                }
            }
        }
    }

    private val maintenanceJob = scheduledExecutorScope.launch {
        while (true) {
            CompletableFuture.runAsync {
                submitPayments()
            }
            delay(50L)
        }
    }

    private fun selectMaxRps(): Double {
        val ownRps = rateLimitPerSec.toDouble()
        val providerRps = 1 / requestAverageProcessingTime.toSeconds().toDouble() * rateLimitPerSec.toDouble()

        return min(ownRps, providerRps)
    }

    private fun getMaxRetries(amount: Int, deadline: Long): AtomicLong {
        val maxRetriesByAmount = (amount / price()).toLong()
        val maxRetriesByDeadline = (deadline - now()) / requestAverageProcessingTime.toMillis()

        return AtomicLong(min(maxRetriesByAmount, maxRetriesByDeadline))
    }
}

fun now() = System.currentTimeMillis()