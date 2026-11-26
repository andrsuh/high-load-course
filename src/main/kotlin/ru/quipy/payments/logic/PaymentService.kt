package ru.quipy.payments.logic

import java.time.Duration
import java.util.*

interface PaymentService {
    /**
     * Admission control: reserves a slot in the queue of every account if each of them is expected
     * to finish the payment before [deadline] (absolute epoch millis).
     * Returns false if any account can't make it in time - the caller should answer 429.
     */
    fun tryReserve(deadline: Long): Boolean

    /**
     * Releases reservations made by [tryReserve] when the payment won't be submitted.
     */
    fun releaseReservation()

    /**
     * Submit payment request to some external service. Requires a successful [tryReserve].
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)
}

/**
 * Adapter for external payment system. Represents the account in the external system.
 *
 * !!! You can extend the interface with additional methods if needed. !!!

 */
interface PaymentExternalSystemAdapter : AutoCloseable {
    override fun close() {}

    /**
     * Reserves a place in the account queue if the payment is expected to complete before [deadline].
     * Every successful reservation must be followed by exactly one [performPaymentAsync] or [releaseReservation].
     */
    fun tryReserve(deadline: Long): Boolean

    fun releaseReservation()

    /**
     * Performs the payment. Requires a reservation obtained via [tryReserve] and releases it on completion.
     */
    fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)

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