package ru.quipy.payments.logic

import java.time.Duration
import java.util.*

interface PaymentService {
    /**
     * Submit payment request to some external service.
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)

    /**
     * How many payments should be processed concurrently to use the accounts at full speed.
     */
    fun parallelism(): Int

    /**
     * Reserve the moment (ms) to send the payment to the provider so that it is processed before the deadline,
     * or null if it can't be.
     */
    fun reserveSendSlot(deadline: Long): Long?

    /**
     * Timestamp (ms) after which it makes sense to retry a rejected payment.
     */
    fun retryAfter(deadline: Long): Long
}

/**
 * Adapter for external payment system. Represents the account in the external system.
 *
 * !!! You can extend the interface with additional methods if needed. !!!

 */
interface PaymentExternalSystemAdapter {
    fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)

    fun parallelism(): Int

    fun reserveSendSlot(deadline: Long): Long?

    fun retryAfter(deadline: Long): Long

    fun name(): String

    fun price(): Int

    fun isEnabled(): Boolean
}

/**
 * Describes properties of payment-provider accounts.
 */
data class PaymentAccountProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val price: Int,
    val averageProcessingTime: Duration = Duration.ofSeconds(11),
    val enabled: Boolean,
)

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val transactionId: String,
    val paymentId: String,
    val result: Boolean,
    val message: String? = null,
)