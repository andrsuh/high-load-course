package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.dto.PendingRequest
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {
    private val scheduledExecutorScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestsQueue = PriorityBlockingQueue<PendingRequest>(parallelRequests)
    private val outgoingRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L))
    private val inFlightRequests = AtomicInteger(0)

    private val coreExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(),
        NamedThreadFactory("payment-submission-executor")
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=${paymentId}&amount=${amount}")
            post(emptyBody)
        }.build()

        val pendingRequest =
            PendingRequest(transactionId, paymentId, amount, paymentStartedAt, deadline, accountName, request)
        requestsQueue.add(pendingRequest)
    }

    override fun submitPayments() {
        val paymentRequest = requestsQueue.poll()

        if (paymentRequest === null) {
            return
        }

        if (inFlightRequests.incrementAndGet() > parallelRequests) {
            inFlightRequests.decrementAndGet()
            requestsQueue.add(paymentRequest)

            return
        }

        if (!outgoingRateLimiter.tick()) {
            inFlightRequests.decrementAndGet()
            requestsQueue.add(paymentRequest)

            return
        }

        coreExecutor.submit {
            try {
                // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
                // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
                paymentESService.update(paymentRequest.paymentId) {
                    it.logSubmission(
                        success = true,
                        paymentRequest.transactionId,
                        now(),
                        Duration.ofMillis(now() - paymentRequest.paymentStartedAt)
                    )
                }

                val body = paymentRequest.call()

                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(body.result, now(), paymentRequest.transactionId, reason = body.message)
                }

                inFlightRequests.decrementAndGet()
            } catch (e: Exception) {
                handleException(paymentRequest, e)
                inFlightRequests.decrementAndGet()
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun canAcceptPayment(deadline: Long): Boolean {
        if (deadline.toDouble() < now().toDouble() + (requestsQueue.size + 1).toDouble() / selectMaxRps() * 1000) {
            throw IllegalStateException("Time limits for $accountName breached")
        }

        return true
    }

    private fun handleException(paymentRequest: PendingRequest, e: Exception) {
        when (e) {
            is SocketTimeoutException -> {
                logger.error(
                    "[$accountName] Payment timeout for txId: ${paymentRequest.transactionId}, payment: ${paymentRequest.paymentId}",
                    e
                )
                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), paymentRequest.transactionId, reason = "Request timeout.")
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

    private val releaseJob = scheduledExecutorScope.launch {
        while (true) {
            submitPayments()
        }
    }

    private fun selectMaxRps(): Double {
        val ownRps = rateLimitPerSec.toDouble()
        val providerRps = 1 / requestAverageProcessingTime.toSeconds().toDouble() * rateLimitPerSec.toDouble()

        return min(ownRps, providerRps)
    }
}

public fun now() = System.currentTimeMillis()