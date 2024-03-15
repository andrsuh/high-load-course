package ru.quipy.payments.logic

import ru.quipy.common.utils.CoroutineRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import java.time.Duration
import java.util.*

interface PaymentService {
    val rateLimiter: CoroutineRateLimiter
    val window: NonBlockingOngoingWindow

    /**
     * Submit payment request to external service.
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long)

    fun canWait(paymentStartedAt: Long): Boolean
    fun notOverTime(paymentStartedAt: Long): Boolean
    fun getSpeed(): Double

    val getCost: Double
}

interface PaymentExternalService : PaymentService {
}

/**
 * Describes properties of payment-provider accounts.
 */
data class ExternalServiceProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val request95thPercentileProcessingTime: Duration = Duration.ofSeconds(11),
    val cost: Double
)

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val result: Boolean,
    val message: String? = null,
)