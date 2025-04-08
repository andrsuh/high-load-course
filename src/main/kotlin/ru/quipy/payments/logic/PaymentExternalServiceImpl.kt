package ru.quipy.payments.logic

import kotlinx.coroutines.*
import java.util.concurrent.Semaphore
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.*
import ru.quipy.common.utils.exceptions.RETRIABLE_PROCESSING_FAIL_REASONS
import ru.quipy.common.utils.exceptions.RequestProcessingException
import ru.quipy.config.SUCCESS_QUANTILE
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.dto.FinishedRequest
import ru.quipy.payments.dto.InFlightRequest
import ru.quipy.payments.dto.PendingRequest
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {
    private val scheduledExecutorScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val urlBuilder = StringBuilder(256)

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(contentType = null)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestsQueue = PriorityBlockingQueue<PendingRequest>(parallelRequests)
    private val outgoingRateLimiter = CompositeRateLimiter(
        SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L)),
        LeakingBucketRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L), selectMaxRps().toInt())
    )
    private val inFlightRequestsLimiter = Semaphore(parallelRequests)

    private val externalSystemAnalyzer = ExternalSystemAnalyzer()

    private val coreExecutor = ThreadPoolExecutor(
        64,
        64,
        0L,
        TimeUnit.MILLISECONDS,
        PriorityBlockingQueue(),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

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
                accountName
            )
        requestsQueue.add(pendingRequest)
    }

    override fun submitPayments() {
        val requests = (1..100).mapNotNull { requestsQueue.poll() }

        requests.forEach { paymentRequest ->
            val url = buildUrl(paymentRequest)
            coreExecutor.execute(createOptimizedRequest(paymentRequest, url))
        }
    }

    private fun buildUrl(request: PendingRequest): String {
        urlBuilder.clear()
        return urlBuilder.apply {
            append("http://localhost:1234/external/process?")
            append("serviceName=$serviceName&")
            append("accountName=$accountName&")
            append("transactionId=${request.transactionId}&")
            append("paymentId=${request.paymentId}&")
            append("amount=${request.amount}&")
            append("timeout=PT${"%.3f".format(requestAverageProcessingTime.toMillis()/1000.00)}s")
        }.toString()
    }

    private fun createOptimizedRequest(paymentRequest: PendingRequest, url: String) =
        object : InFlightRequest(paymentRequest, paymentESService, url,
            requestAverageProcessingTime.toMillis(), ::handleException) {

            override fun run() = runBlocking {
                try {
                    outgoingRateLimiter.tickBlocking()
                    inFlightRequestsLimiter.acquireUninterruptibly()
                    super.run()
                    withContext(Dispatchers.IO) {
                        externalSystemAnalyzer.addFinished(FinishedRequest(processingDuration))
                    }
                } catch (e: Throwable) {
                    handleException(paymentRequest, e)
                } finally {
                    inFlightRequestsLimiter.release()
                }
            }
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
                logger.error(
                    "[$accountName] Payment timeout for txId: ${paymentRequest.transactionId}, payment: ${paymentRequest.paymentId}"
                )
                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), paymentRequest.transactionId, reason = "Request timeout.")
                }
            }

            is RequestProcessingException -> {
                logger.error(
                    "[$accountName] Request processing failed for txId: ${paymentRequest.transactionId}, payment: ${paymentRequest.paymentId}. Reason: ${e.failReason}. Message: ${e.message}"
                )

                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), paymentRequest.transactionId, reason = e.message)
                }

                if (RETRIABLE_PROCESSING_FAIL_REASONS.contains(e.failReason)) {
                    paymentRequest.setRetryAfter((paymentRequest.deadline - now()) / (paymentRequest.maxRetries.get() - paymentRequest.getRetries()))
                    requestsQueue.add(paymentRequest)
                }
            }

            else -> {
                logger.error(
                    "[$accountName] Payment failed for txId: ${paymentRequest.transactionId}, payment: ${paymentRequest.paymentId}",
                    e
                )

                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), paymentRequest.transactionId, reason = e.message)
                }
            }
        }
    }

    private fun cleanup() {
        while (requestsQueue.peek() !== null && requestsQueue.peek().deadline + requestAverageProcessingTime.toMillis() < now()) {
            val paymentRequest = requestsQueue.poll()

            paymentESService.update(paymentRequest.paymentId) {
                it.logProcessing(false, now(), paymentRequest.transactionId, reason = "Timeout")
            }
        }
    }

    private val maintenanceJob = scheduledExecutorScope.launch {
        while (true) {
            val startTime = System.currentTimeMillis()

            // Чередуем выполнение задач
            if (Random.nextBoolean()) {
                submitPayments()
            } else {
                cleanup()
            }

            val processingTime = System.currentTimeMillis() - startTime
            delay(maxOf(10L, 50L - processingTime))
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

    private fun getTimeout(): Long {
        var timeout = externalSystemAnalyzer.getQuantile(SUCCESS_QUANTILE)?.processingDuration
            ?: requestAverageProcessingTime.toMillis()
        timeout = min(timeout, 2 * requestAverageProcessingTime.toMillis())
        timeout = max(timeout, requestAverageProcessingTime.toMillis() / 2)

        return timeout
    }
}

fun now() = System.currentTimeMillis()